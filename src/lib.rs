extern crate snap;
extern crate byteorder;
extern crate thrift;
extern crate ordered_float;
extern crate try_from;

mod parquet;
mod encodings;
mod levels;

use std::fs::{OpenOptions, File};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom, Result, Take, Read};
use thrift::protocol::{TCompactInputProtocol};
use parquet::*;
use byteorder::{LittleEndian, ReadBytesExt};

const MAGIC: &'static str = "PAR1";

pub fn open(file_name: &str) -> Result<BufReader<File>> {
    let unbuffered = OpenOptions::new().read(true).open(file_name)?;
    let buffered = BufReader::new(unbuffered);
    Ok(buffered)
}

pub fn read_file_meta(buffered: &mut BufReader<File>) -> Result<FileMetaData> {
    validate_magic(buffered)?;

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

    Ok(file_meta)
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

struct ColumnPagesIter<'a> {
    file: &'a mut BufReader<File>,
    row_groups: &'a Vec<RowGroup>,
    column_idx: usize,
}

impl<'a> ColumnPagesIter<'a> {
    pub fn new(file: &'a mut BufReader<File>, file_meta: &'a FileMetaData, column: &str) -> ColumnPagesIter<'a> {
        let row_groups = &file_meta.row_groups;

        let column_idx = file_meta.schema.iter().
            position(|s| {s.name == column}).expect("Column not found")
            // FIXME: position must be in leaf-order walk
            // Temp fix, skip root schema node
            - 1;

        //println!("{:#?}", file_info.file_meta.schema);

        ColumnPagesIter {file, row_groups, column_idx}
    }

    fn read_page_header(&mut self, file: &mut Take<impl Read>) -> PageHeader {
        //println!("Reading page @{:?}", self.file);
        let mut protocol = TCompactInputProtocol::new(file);
        let page_header = PageHeader::read_from_in_protocol(&mut protocol).
            expect("Failed to deserialize ColumnMetaData");
        println!("{:#?}", page_header);

        page_header
    }

    fn iter(&'a mut self) -> impl 'a + Iterator<Item=PageHeader> {
        self.row_groups.iter().
        flat_map(|rg| {
            rg.columns.iter()
        }).map(move |col_chunk| {
            
            let offset = col_chunk.file_offset;
            println!("Setting chunk offset={}", offset);
            self.file.seek(SeekFrom::Start(offset as u64)).expect("Failed to seek to column metadata");

            let column_chunk_size = col_chunk.meta_data.as_ref().
                expect("ColumnChunk does not have metadata").total_compressed_size;
            let wrapper = self.file.take(column_chunk_size as u64);
            let mut protocol = TCompactInputProtocol::new(wrapper);
            let page_header = PageHeader::read_from_in_protocol(&mut protocol).
                expect("Failed to deserialize ColumnMetaData");
            println!("{:#?}", page_header);
            

            page_header
        })
    }

    /*fn read_page(page_header: &PageHeader) {
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
    }*/
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_get_metadata() {
        let mut file = open(&"test-data/test1.snappy.parquet".to_string()).expect("Failed to read parquet file");
        let meta = read_file_meta(&mut file).expect("Failed to read meta");
        println!("Version {}, rows: {}, row_groups: {}\n    created_by {:?}",
                 meta.version,
                 meta.num_rows,
                 meta.row_groups.len(),
                 meta.created_by
        );
    }

    #[test]
    fn column_pages_iterator() {
        let mut file = open(&"test-data/test1.snappy.parquet").
            expect("Failed to read parquet file");
        let meta = read_file_meta(&mut file).unwrap();
        let mut it = ColumnPagesIter::new(&mut file,&meta, &"id");

        let count = it.iter()
            .count();
        println!("Page count: {}", count);
    }
}
