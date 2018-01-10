extern crate thrift;
extern crate ordered_float;
extern crate try_from;

mod parquet;

use std::fs::{OpenOptions, File};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom, Result};
use thrift::protocol::{TCompactInputProtocol};
use parquet::*;


const MAGIC: &'static str ="PAR1";

pub struct Reader {
    pub info: Info,
    file_meta: FileMetaData,
    pub protocol: TCompactInputProtocol<BufReader<File>>
}
pub struct Info {
    pub version: i32,
    pub num_rows: i64,
    pub row_groups: usize,
    pub created_by: String
}

impl Reader {
    pub fn open(file_name: &String) -> Result<Reader> {
        let unbuffered = OpenOptions::new().read(true).open(file_name)?;
        let mut buffered = BufReader::new(unbuffered);

        validate_magic(&mut buffered)?;
        
        // read footer metadata length and magic
        buffered.seek(SeekFrom::End(-(4 + 4)))?;
        let mut buf4: [u8; 4] = [0; 4];
        buffered.read_exact(buf4.as_mut())?;
        let footer_len: u32 = (buf4[0] as u32) | (buf4[1] as u32) << 8 | (buf4[2] as u32) << 16 | (buf4[3] as u32) << 24;

        buffered.seek(SeekFrom::Current(-(footer_len as i64 +8_i64))).expect("File metadata position failed");

        let mut protocol = TCompactInputProtocol::new(buffered);
        let file_meta = FileMetaData::read_from_in_protocol(&mut protocol).expect("Failed to deserialize file metadata");

        Ok(Reader {
            info: Info {
                version: file_meta.version,
                num_rows: file_meta.num_rows,
                row_groups: file_meta.row_groups.len(),
                created_by: file_meta.created_by.clone().unwrap_or(String::new())
            },
            file_meta,
            protocol
        })
    }

}

/**
    Validate magic 'PAR1' at start and end of file
*/
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

impl IntoIterator for Reader {
    type Item = i32;
    type IntoIter = Iter;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            group: 0,
            column: 0,
            group_len: self.file_meta.row_groups.len(),
            column_len: self.file_meta.row_groups[0].columns.len(),
            reader: self
        }
    }
}

#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct Iter {
    group: usize,
    column: usize,
    group_len: usize,
    column_len: usize,
    reader: Reader
}

impl Iterator for Iter {
    type Item = i32;
    fn next(&mut self) -> Option<i32> {
        if self.group >= self.group_len {
            None
        } else {
            let group;
            let column;
            if self.column >= self.column_len {
                self.column = 0;
                self.group += 1;
                if self.group >= self.group_len {
                    return None;
                }
                group = &self.reader.file_meta.row_groups[self.group];
                column = &group.columns[self.column];
                self.column_len = group.columns.len(); // can column count be different in different column chunk?
            } else {
                group = &self.reader.file_meta.row_groups[self.group];
                column = &group.columns[self.column];
            }

            self.reader.protocol.seek(SeekFrom::Start(column.file_offset as u64)).expect("Failed to seek to column metadata");
            let page_header = PageHeader::read_from_in_protocol(&mut self.reader.protocol).expect("Failed to deserialize ColumnMetaData");
            println!("PageHeader: {:?}", page_header);

            self.column += 1;
            Some(1)
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_get_metadata() {
        let reader = Reader::open(&"test-data/test1.snappy.parquet".to_string()).expect("Failed to read parquet file");
        println!("Version {}, rows: {}, row_groups: {}\n    created_by {:?}",
                 reader.info.version,
                 reader.info.num_rows,
                 reader.info.row_groups,
                 reader.info.created_by
        );

    }

    #[test]
    fn iterator() {
        let reader = Reader::open(&"test-data/test1.snappy.parquet".to_string()).expect("Failed to read parquet file");
        for row in reader {
            //println!("Row: {}", row);
        }
    }
}
