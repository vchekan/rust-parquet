extern crate snap;
extern crate byteorder;
extern crate thrift;
extern crate ordered_float;
extern crate try_from;

mod parquet;
mod encodings;
mod levels;
mod page;

use std::fs::{OpenOptions, File};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom, Result};
use thrift::protocol::{TCompactInputProtocol};
use parquet::*;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};

use encodings::BitPackingRleReader;
use page::PageIter;


const MAGIC: &'static str ="PAR1";

pub struct FileInfo {
    file_meta: FileMetaData,
    pub protocol: TCompactInputProtocol<BufReader<File>>

}

impl FileInfo {
    pub fn open(file_name: &String) -> Result<FileInfo> {
        let unbuffered = OpenOptions::new().read(true).open(file_name)?;
        let mut buffered = BufReader::new(unbuffered);

        validate_magic(&mut buffered)?;
        
        // read footer metadata length and magic
        // TODO: BufReader's seek reset buffers even if seek is within buffered range :(
        buffered.seek(SeekFrom::End(-(4 + 4)))?;
        let footer_len = buffered.read_u32::<LittleEndian>()?;
        println!("footer_len: {}", footer_len);

        buffered.seek(SeekFrom::End(-(footer_len as i64 +8_i64))).expect("File metadata position failed");

        let mut protocol = TCompactInputProtocol::new(buffered);
        let file_meta = FileMetaData::read_from_in_protocol(&mut protocol).
            expect("Failed to deserialize file metadata");
        //println!("{:#?}", file_meta);

        Ok(FileInfo {
            file_meta,
            protocol
        })
    }

    pub fn iter(&mut self) -> ColumnIter {
        ColumnIter::new(self)
    }

    pub fn version(&self) -> i32 {
        self.file_meta.version
    }

    pub fn num_rows(&self) -> i64 {
        self.file_meta.num_rows
    }

    pub fn row_groups(&self) -> usize {
        self.file_meta.row_groups.len()
    }

    pub fn created_by(&self) -> Option<&String> {
        self.file_meta.created_by.as_ref()
    }
}

///    Validate magic 'PAR1' at start and end of file
fn validate_magic(file: &mut BufReader<File>) -> Result<()> {
    let mut buf4: [u8; 4] = [0; 4];

    file.seek(SeekFrom::Start(0))?;
    file.read_exact(buf4.as_mut())?;
    if MAGIC.as_bytes().ne(&buf4) {
        return Err(std::io::Error::new(io::ErrorKind::Other, "Bad magic at file start"));
    }

    file.seek(SeekFrom::End(-4))?;
    file.read_exact(buf4.as_mut())?;
    if MAGIC.as_bytes().ne(&buf4) {
        return Err(std::io::Error::new(io::ErrorKind::Other, "Bad magic at file end"));
    }

    Ok(())
}

#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct ColumnIter<'a> {
    group: usize,
    column: usize,
    reader: &'a mut FileInfo,
    next_page_offset: u64,

    buffer: Vec<u8>,
    data_offset: usize,
}

impl<'a> ColumnIter<'a>  {
    fn new(meta: &mut FileInfo) -> ColumnIter {

        // Get 1st page offset
        let next_page_offset;
        {
            let group = &meta.file_meta.row_groups[0];
            let column = &group.columns[0];
            next_page_offset = column.file_offset as u64;
        }

        //println!("Column: {:#?}", column);
        //let column_meta = column.meta_data.as_ref().expect("Column metadata is empty");

        let mut iter = ColumnIter {
            group: 0,
            column: 0,
            reader: meta,
            next_page_offset,
            buffer: Vec::new(),
            data_offset: 0,
        };

        iter.next_page();
        iter
    }

    fn next_page(&mut self) -> Result<()> {
        let group = &self.reader.file_meta.row_groups[self.group];
        let column = &group.columns[self.column];
        let column_meta = column.meta_data.as_ref().
            expect("Column metadata is empty");

        //
        // Read page header
        //
        println!("Reading page @{}", self.next_page_offset);
        self.reader.protocol.inner().seek(SeekFrom::Start(self.next_page_offset)).
            expect("Failed to seek to column metadata");
        let page_header = PageHeader::read_from_in_protocol(&mut self.reader.protocol).
            expect("Failed to deserialize ColumnMetaData");
        //println!("{:#?}", page_header);

        //
        // Decompress page data
        //
        // TODO: check page size for sanity
        match column_meta.codec {
            parquet::CompressionCodec::UNCOMPRESSED => {
                self.buffer.resize(page_header.compressed_page_size as usize, 0);
                self.reader.protocol.inner().read_exact(self.buffer.as_mut())?;
                println!("Read uncompressed page");
            },
            parquet::CompressionCodec::SNAPPY => {
                let mut page_data_compressed = vec![0_u8; page_header.compressed_page_size as usize];
                self.reader.protocol.inner().read_exact(&mut page_data_compressed)?;

                self.buffer.resize(page_header.uncompressed_page_size as usize, 0);
                let res = snap::Decoder::new().decompress(&page_data_compressed, self.buffer.as_mut())?;
                println!("Snappy read: {}", res);
            },
            // TODO
            _ => unimplemented!("this compression is not implemented yet: {:?}", column_meta.codec)
        };
        println!("page_data (un-compressed)[{}]: {:?}", self.buffer.len(), self.buffer[0..100.min(self.buffer.len())].as_ref());


        let mut data_offset;
        {
            //
            // Decode repetition and definition levels
            //
            //let repetition_levels = RleReader::new(1, buff.as_slice()).expect("Failed to parse repetition levels");
            let definition_levels = BitPackingRleReader::new(1, self.buffer[0..].as_ref()).
                expect("Failed to parse definition levels");
            //println!("definition_levels: {:?}", definition_levels);
            data_offset = definition_levels.next;
            println!("data_offset: {}", data_offset);
        }

        self.next_page_offset += page_header.compressed_page_size as u64;
        self.data_offset = data_offset as usize;

        Ok(())
    }
}

impl<'a> Iterator for ColumnIter<'a> {
    // TODO: what's the right  way to act if error in format in iterator?

    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        println!(">>> self.data_offset > self.buffer.len(): {} > {}", self.data_offset, self.buffer.len());
        if self.data_offset > self.buffer.len() {
            println!("next: {} > {}", self.data_offset, self.buffer.len());
            //let next_page_result = self.next_page();
            //if !next_page_result.is_ok() {
            if let Err(e) = self.next_page() {
                println!("next_page error: {:?}", e);
                return None
            }
        } else {
            return None
        }

        let current_data = &self.buffer[self.data_offset..];
        let res = LittleEndian::read_i64(current_data);
        self.data_offset += 8;
        Some(res)
    }
}

// TODO:
fn max_definition_levels(path: Vec<String>) -> i32 {
    1
}
fn max_repetition_level() -> i32 {1}


struct RowGroupIter<'a> {
    row_groups: &'a Vec<RowGroup>,
    row_group_idx: usize
}

impl<'a> RowGroupIter<'a> {
    pub fn new(row_groups: &'a Vec<RowGroup>) -> RowGroupIter<'a> {
        RowGroupIter { row_groups, row_group_idx: 0}
    }
}

impl<'a> Iterator for RowGroupIter<'a> {
    type Item = &'a RowGroup;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.row_group_idx < self.row_groups.len() {
            let res = &self.row_groups[self.row_group_idx];

            self.row_group_idx += 1;
            Some(res)
        } else {
            None
        }
    }
}

struct ColumnPagesIter<'a> {
    protocol: &'a mut TCompactInputProtocol<BufReader<File>>,
    row_group_iter: RowGroupIter<'a>,
    column_idx: usize,
    // Offset of start of current page in file
    next_page_offset: i64,
    eof_row_group_offset: i64,
}

impl<'a> ColumnPagesIter<'a> {
    pub fn new(file_meta: &'a mut FileInfo, column: &String) -> ColumnPagesIter<'a> {
        let row_group_iter = RowGroupIter::new(&file_meta.file_meta.row_groups);

        let column_idx = file_meta.file_meta.schema.iter().
            position(|s| {&s.name == column}).expect("Column not found");

        let protocol = &mut file_meta.protocol;

        ColumnPagesIter {protocol, row_group_iter, column_idx, next_page_offset: 0_i64, eof_row_group_offset: 0_i64}
    }

    fn read_page_header(&mut self) -> PageHeader {
        println!("Reading page @{}", self.next_page_offset);
        self.protocol.inner().seek(SeekFrom::Start(self.next_page_offset as u64)).
            expect("Failed to seek to column metadata");
        let page_header = PageHeader::read_from_in_protocol(self.protocol).
            expect("Failed to deserialize ColumnMetaData");
        println!("{:#?}", page_header);

        println!("read_page_header: page offset {}->{}/{}",
            self.next_page_offset,
            self.next_page_offset + page_header.compressed_page_size as i64,
            page_header.compressed_page_size
        );

        self.next_page_offset += page_header.compressed_page_size as i64;

        page_header
    }
}

impl<'a> Iterator for ColumnPagesIter<'a> {
    type Item = PageHeader;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.next_page_offset < self.eof_row_group_offset {
            Some(self.read_page_header())
        } else {
            match self.row_group_iter.next() {
                None => None,
                Some(row_group) => {
                    let column_chunk = &row_group.columns[self.column_idx];
                    self.next_page_offset = column_chunk.file_offset;
                    self.eof_row_group_offset = column_chunk.file_offset + row_group.total_byte_size + 1_i64;
                    self.next()
                }
            }
        }

    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_get_metadata() {
        let fileMeta = FileInfo::open(&"test-data/test1.snappy.parquet".to_string()).expect("Failed to read parquet file");
        println!("Version {}, rows: {}, row_groups: {}\n    created_by {:?}",
                 fileMeta.version(),
                 fileMeta.num_rows(),
                 fileMeta.row_groups(),
                 fileMeta.created_by()
        );
    }

    #[test]
    fn column_pages_iterator() {
        let mut fileMeta = FileInfo::open(&"test-data/test1.snappy.parquet".to_string()).expect("Failed to read parquet file");
        let it = ColumnPagesIter::new(&mut fileMeta, &"id".to_string());

        let count = it.count();
        println!("Count: {}", count);
    }

        #[test]
    fn row_group_iterator() {
        let mut fileMeta = FileInfo::open(&"test-data/test1.snappy.parquet".to_string()).expect("Failed to read parquet file");
        let row_group_it = RowGroupIter::new(&fileMeta.file_meta.row_groups);
        let count = row_group_it.count();
        println!("Count: {}", count);

        let rows: i64 = RowGroupIter::new(&fileMeta.file_meta.row_groups).
            map(|g| {g.num_rows}).sum();
        println!("Total rows: {}", rows);



        /*for page in PageIter::new(&mut fileMeta.protocol, 4_u64) {
            println!("Crc: {:?}", page.crc);
        }*/

        /*
        let record = parquet!({
            a: {
                a1: i32,
                a2: String
            },
            b: bool
        });
        */

        /*for row in fileMeta.iter().take(10) {
            println!("Row: {}", row);
        }*/

        /*let count = fileMeta.iter().count();
        assert_eq!(count, 131074);
        println!("Count: {}", count);*/
    }

    /*
    #[test]
    fn client() {
        let record = parquet!{
            reddit {
                { id: u64,
                  body: String,
                }
            }
        };

        let fileMeta = FileInfo::open(&"test-data/test1.snappy.parquet".to_string()).expect("Failed to read parquet file");
        fileMeta.read().take(100)(|record| println!("{}", record));
    }
    */
}
