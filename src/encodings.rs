
// TODO: use mod byteorder to read little endian
#[derive(Debug)]
pub struct RleReader<'a> {
    bit_width: u32,
    compressed_len: u32,
    data: &'a [u8],
}

impl<'a> RleReader<'a> {
    pub fn new(max_level: u32, data: &'a [u8]) -> Result<RleReader,String> {
        if data.len() < 4 {return Err("Failed to read RLE encoding length".to_string())}

        let len_encoded: u32 = (data[3] as u32) << 8*3 | (data[2] as u32) << 8*2 | (data[1] as u32) << 8 | data[0] as u32;
        println!(">>> len_encoded: {0}", len_encoded);
        let mut pos = 4;

        if (pos + len_encoded) as usize > data.len() {
            return Err(format!("Unexpected end of RLE data. Declared length {} but got {}",
               len_encoded,
               data.len()-4
            ))
        }

        let bit_width = bit_width(max_level);
        let header: u32 = read_leb128(data, 4);
        let mode = if header & 1 == 1 { Mode::Packed } else { Mode::Rle };
        println!("Pack mode: {:?}", mode);
        match mode {
            Mode::Rle => {
                let repeated = header >> 1;
                let val = read_bitpack_int(bit_width, data);
            }
            Mode::Packed => {
                //return Err("Mode::Packed not implemented yet".to_string());
                unimplemented!("Mode::Packed")
            }
        }

        Ok(RleReader {
            bit_width,
            compressed_len: 4 + len_encoded,
            data,
        })
    }
 }

impl<'a> IntoIterator for RleReader<'a> {
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

fn read_leb128(data: &[u8], offset: u32) -> u32 {
    let mut res = 0_u32;
    let mut pos = offset as usize;
    let mut shift = 0;

    loop {
        let byte = data[pos];
        res |= (byte as u32 & 0x7f) << shift;
        shift += 7;
        pos += 1;
        if (byte as u32 & 0x80) != 0 {
            break res
        }
    }
}

fn read_bitpack_int(bit_width: u32, data: &[u8]) -> Result<i32,String> {
    let byte_len = round_to_byte(bit_width) as usize;

    if byte_len > data.len() { return Err(format!("Too small buffer to unpack int. Int len: {} but buffer len: {}", byte_len, data.len()))}

    match byte_len {
        0 => Ok(0),
        1 => Ok(data[0] as i32),
        2 => Ok(data[0] as i32| (data[1] as i32) << 8),
        3 => Ok(data[0] as i32| (data[1] as i32) << 8 | (data[2] as i32) << 8*2),
        4 => Ok(data[0] as i32| (data[1] as i32) << 8 | (data[2] as i32) << 8*2 | (data[3] as i32) << 8*3),
        _ => Err(format!("Can not handle packed int longer than 4. Got {}", bit_width))
    }
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