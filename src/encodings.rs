use byteorder::{ByteOrder, LittleEndian};

#[derive(Debug)]
pub struct BitPackingRleReader<'a> {
    bit_width: u32,
    compressed_len: u32,
    data: &'a [u8],
    pub next: u32,
}

impl<'a> BitPackingRleReader<'a> {
    pub fn new(max_level: u32, data: &'a [u8]) -> Result<BitPackingRleReader,String> {
        if data.len() < 4 {return Err("Failed to read RLE encoding length".to_string())}

        let len_encoded = LittleEndian::read_u32(data);
        println!(">>> len_encoded: {0}", len_encoded);
        let mut pos = 4_usize;

        if pos + len_encoded as usize > data.len() {
            return Err(format!("Unexpected end of RLE data. Declared length {} but got {}",
               len_encoded,
               data.len()-4
            ))
        }

        let header: u32 = read_leb128(data, &mut pos);
        let mode = if header & 1 == 1 { Mode::Packed } else { Mode::Rle };
        let bit_width = bit_width(max_level);
        println!("Pack mode: {:?} header: {}", mode, header);
        match mode {
            Mode::Rle => {
                let repeated = header >> 1;
                let val = read_bitpack_int(bit_width, data, & mut pos).expect("Failed to decode RLE value");
                println!("RLE decoding: repeated: {} val: {}", repeated, val);
            }
            Mode::Packed => {
                //return Err("Mode::Packed not implemented yet".to_string());
                unimplemented!("Mode::Packed")
            }
        }

        Ok(BitPackingRleReader {
            bit_width,
            compressed_len: 4 + len_encoded,
            data,
            next: 4 + len_encoded,
        })
    }
 }

impl<'a> IntoIterator for BitPackingRleReader<'a> {
    type Item = i32;
    type IntoIter = RleIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        RleIter {
            ptr: 0,
            counter: 0,
            data: &self.data[3 .. (self.compressed_len + 4) as usize],
            value: 0,
        }
    }
}

pub struct RleIter<'a> {
    ptr: u32,
    counter: i32,
    value: u32,
    data: &'a [u8],
}

impl<'a> Iterator for RleIter<'a> {
    type Item = i32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.counter == 0 {
            self.read_next()
        }

        if self.counter == -1 {
            return None
        }

        None
    }
}

impl<'a> RleIter<'a> {
    fn read_next(&mut self) {
        
    }
}

//
// Packed Reader
//

pub struct BitPackingReader<'a> {
    compressed_len: u32,
    data: &'a [u8],
}

impl<'a> BitPackingReader<'a> {
    fn new(data: &'a [u8]) -> Result<BitPackingReader,String> {
        if data.len() < 4 {return Err("Bit packing read error: can't read size".to_string())}

        let compressed_len = LittleEndian::read_u32(data);
        Ok(BitPackingReader {
            compressed_len,
            data,
        })
    }
}

#[derive(Debug)]
enum Mode {
    Rle,
    Packed
}

fn bit_width(max_int: u32) -> u32 {
    32 - max_int.leading_zeros()
}

fn round_to_byte(bits: u32) -> u32 {
    (bits + 7) / 8
}

fn read_leb128(data: &[u8], offset: &mut usize) -> u32 {
    let mut res = 0_u32;
    let mut pos = *offset;
    let mut shift = 0;

    let res = loop {
        let byte = data[pos];
        res |= (byte as u32 & 0x7f) << shift;
        shift += 7;
        pos += 1;
        if (byte as u32 & 0x80) == 0 {
            break res
        }
    };

    *offset = pos;
    res
}

fn read_bitpack_int(bit_width: u32, data: &[u8], offset: &mut usize) -> Result<i32,String> {
    let byte_len = round_to_byte(bit_width) as usize;

    if byte_len > data.len() { return Err(format!("Too small buffer to unpack int. Int len: {} but buffer len: {}", byte_len, data.len()))}

    let res = match byte_len {
        0 => Ok(0),
        1 => Ok(data[*offset] as i32),
        2 => Ok(LittleEndian::read_i16(&data[*offset..]) as i32),
        3 => Ok(LittleEndian::read_i24(&data[*offset..]) as i32),
        4 => Ok(LittleEndian::read_i32(&data[*offset..])),
        _ => Err(format!("Can not handle packed int longer than 4 bytes. Got {}", bit_width))
    };

    if res.is_ok() {
        *offset += byte_len;
    }

    res
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_width_test() {
        assert_eq!(1, bit_width(1));
        assert_eq!(4, bit_width(9));
    }
}