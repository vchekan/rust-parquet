extern crate snap;

use parquet;
use thrift::protocol::{TCompactInputProtocol};
use std::fs::{File};
use std::io::{BufReader, SeekFrom};
use std::io::prelude::*;

pub struct PageIter<'a> {
    protocol: &'a mut TCompactInputProtocol<BufReader<File>>,
}

impl<'a> PageIter<'a> {
    pub fn new(protocol: &mut TCompactInputProtocol<BufReader<File>>, next_page_offset: u64) -> PageIter {
        PageIter {
            protocol,
        }
    }
}

impl<'a> Iterator for PageIter<'a> {
    type Item = parquet::PageHeader;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        //
        // Read page header
        //
        //println!("Reading page @{}", self.next_page_offset);
        /*self.protocol.inner().seek(SeekFrom::Start(self.next_page_offset)).
            expect("Failed to seek to column metadata");
        let page_header = parquet::PageHeader::read_from_in_protocol(self.protocol).
            expect("Failed to deserialize ColumnMetaData");
        //println!("{:#?}", page_header);

        self.protocol.inner().seek(SeekFrom::Current(page_header.compressed_page_size as i64)).
            expect("Failed to seek next page");


        Some(page_header)*/
        None
    }
}