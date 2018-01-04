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

fn read(file_name: String) -> std::io::Result<()> {
    let mut foptions = OpenOptions::new();
    let mut unbuffered = foptions.read(true).open(file_name)?;
    let mut buffered = BufReader::new(unbuffered);


    // read footer metadta length and magic
    buffered.seek(SeekFrom::End(-(4 + 4)))?;
    let mut buf8: [u8; 4] = [0; 4];
    buffered.read(buf8.as_mut())?;
    let footer_len: u32 = (buf8[0] as u32) | (buf8[1] as u32) << 8 | (buf8[2] as u32) << 16 | (buf8[3] as u32) << 24;
    println!("footer len: {}, {:?}", footer_len, buf8);

    // magic
    buffered.read_exact(buf8.as_mut())?;
    if MAGIC.as_bytes().ne(&buf8) {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Bad format"));
    }

    // TODO: check footer size for sanity
    let pos = buffered.seek(SeekFrom::Current(-(footer_len as i64 +8_i64))).expect("File metadata position failed");

    let mut protocol = TCompactInputProtocol::new(buffered);
    let fileMeta = FileMetaData::read_from_in_protocol(& mut protocol).expect("Failed to deserialize file metadata");
    println!("Version {}, rows: {}, row_groups: {}\n    created_by {:?}\n    key_value_metadata: {:?}",
        fileMeta.version,
        fileMeta.num_rows,
        fileMeta.row_groups.len(),
        fileMeta.created_by,
        fileMeta.key_value_metadata
    );

    //fileMeta.row_groups.get(0).expect("No row group found")

    return Ok(());
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        read("test-data/test1.snappy.parquet".to_string()).expect("Failed to read paruet file");
    }
}
