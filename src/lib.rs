/*struct readConfig {
    chunkHandler
    columnHandler
    pageHandler
    valuesHandler
}*/

use std::fs::{OpenOptions};
use std::io::prelude::*;
use std::io::{SeekFrom};

const magic: &'static str ="PAR1";

fn read(fileName: String) -> std::io::Result<()> {
    let mut foptions = OpenOptions::new();
    let mut f = foptions.read(true).open(fileName)?;


    // read footer metadta length and magic
    f.seek(SeekFrom::End(-(4 + 4)))?;
    let mut buf: [u8; 4] = [0; 4];
    f.read(buf.as_mut())?;
    let footer_len: u32 = (buf[0] as u32) | (buf[1] as u32) << 8 | (buf[2] as u32) << 16 | (buf[3] as u32) << 24;
    println!("footer len: {}, {:?}", footer_len, buf);

    // magic
    f.read(buf.as_mut())?;

    if magic.as_bytes().ne(&buf) {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Bad format"));
    }

    return Ok(());
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {
        //assert_eq!(2 + 2, 4);
        read("test-data/test1.snappy.parquet".to_string());
    }
}
