/*struct readConfig {
    chunkHandler
    columnHandler
    pageHandler
    valuesHandler
}*/

extern crate thrift;
extern crate ordered_float;
extern crate try_from;

mod parquet;

use std::fs::{OpenOptions};
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use thrift::protocol::{TCompactInputProtocol};
use parquet::*;


const MAGIC: &'static str ="PAR1";

pub struct Reader {
    pub info: Info,
    file_meta: FileMetaData,
}
pub struct Info {
    pub version: i32,
    pub num_rows: i64,
    pub row_groups: usize,
    pub created_by: String
}

impl Reader {
    pub fn open(file_name: String) -> std::io::Result<Reader> {
        let mut foptions = OpenOptions::new();
        let mut unbuffered = foptions.read(true).open(file_name)?;
        let mut buffered = BufReader::new(unbuffered);

        // read footer metadata length and magic
        buffered.seek(SeekFrom::End(-(4 + 4)))?;
        let mut buf4: [u8; 4] = [0; 4];
        buffered.read_exact(buf4.as_mut())?;
        let footer_len: u32 = (buf4[0] as u32) | (buf4[1] as u32) << 8 | (buf4[2] as u32) << 16 | (buf4[3] as u32) << 24;
        println!("footer len: {}, {:?}", footer_len, buf4);

        // magic
        buffered.read_exact(buf4.as_mut())?;
        if MAGIC.as_bytes().ne(&buf4) {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Bad format"));
        }

        // TODO: check footer size for sanity
        let pos = buffered.seek(SeekFrom::Current(-(footer_len as i64 +8_i64))).expect("File metadata position failed");

        let mut protocol = TCompactInputProtocol::new(buffered);
        let fileMeta = FileMetaData::read_from_in_protocol(& mut protocol).expect("Failed to deserialize file metadata");

        return Ok(Reader {
            info: Info {
                version: fileMeta.version,
                num_rows: fileMeta.num_rows,
                row_groups: fileMeta.row_groups.len(),
                created_by: fileMeta.created_by.clone().unwrap_or(String::from(""))
            },
            file_meta: fileMeta
        });
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        let reader = Reader::open("test-data/test1.snappy.parquet".to_string()).expect("Failed to read parquet file");
        println!("Version {}, rows: {}, row_groups: {}\n    created_by {:?}",
                 reader.info.version,
                 reader.info.num_rows,
                 reader.info.row_groups,
                 reader.info.created_by
        );

    }
}
