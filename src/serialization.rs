use crate::{Result, StorageError};

// serializer for writing data
pub struct Serializer {
    buffer: Vec<u8>,
}

impl Serializer {
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    pub fn write_u8(&mut self, value: u8) {
        self.buffer.push(value);
    }
    pub fn write_u16(&mut self, value: u16) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }
    pub fn write_u32(&mut self, value: u32) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }
    pub fn write_u64(&mut self, value: u64) {
        self.buffer.extend_from_slice(&value.to_le_bytes());
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.buffer.extend_from_slice(bytes);
    }

    pub fn write_string(&mut self, s: &str) {
        let bytes = s.as_bytes();
        self.write_u32(bytes.len() as u32);
        self.write_bytes(bytes);
    }

    pub fn write_variable_int(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.write_u8((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
        self.write_u8(value as u8);
    }

    pub fn size(&self) -> usize {
        self.buffer.len()
    }

    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }
}

impl Default for Serializer {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Deserializer<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> Deserializer<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    fn ensure_byte(&self, n: usize) -> Result<()> {
        if self.position + n > self.data.len() {
            Err(StorageError::corruption("unexpected end of data"))
        } else {
            Ok(())
        }
    }

    // read u8 value
    pub fn read_u8(&mut self) -> Result<u8> {
        self.ensure_byte(1)?;
        let value = self.data[self.position];
        self.position += 1;
        Ok(value)
    }

    pub fn read_u16(&mut self) -> Result<u16> {
        self.ensure_byte(2)?;
        let bytes = &self.data[self.position..self.position + 2];
        self.position += 2;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        self.ensure_byte(4)?;
        let bytes = &self.data[self.position..self.position + 4];
        self.position += 4;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn read_u64(&mut self) -> Result<u64> {
        self.ensure_byte(8)?;
        let bytes = &self.data[self.position..self.position + 8];
        self.position += 8;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.ensure_byte(len)?;
        let bytes = &self.data[self.position..self.position + len];
        self.position += len;
        Ok(bytes)
    }

    pub fn read_string(&mut self) -> Result<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;

        String::from_utf8(bytes.to_vec())
            .map_err(|_| StorageError::corruption("Invalid UTF-8 string"))
    }

    pub fn read_variable_int(&mut self) -> Result<u64> {
        let mut result = 0_u64;
        let mut shift = 0;

        loop {
            if shift >= 64 {
                return Err(StorageError::corruption("variable integer too long"));
            }

            let byte = self.read_u8()?;
            result |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                break;
            }

            shift += 7;
        }

        Ok(result)
    }
    pub fn skip(&mut self, n: usize) -> Result<()> {
        self.ensure_byte(n)?;
        self.position += n;
        Ok(())
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn remaining(&self) -> usize {
        self.data.len() - self.position
    }

    pub fn is_eof(&self) -> bool {
        self.position >= self.data.len()
    }

    pub fn remaining_slice(&self) -> &'a [u8] {
        &self.data[self.position..]
    }

    pub fn peek_u8(&self) -> Result<u8> {
        self.ensure_byte(1)?;
        Ok(self.data[self.position])
    }
}

pub trait Serialize {
    fn serialize(&self, serializer: &mut Serializer) -> Result<()>;
}

pub trait Deserialize<'a>: Sized {
    fn deserialize(deserializer: &mut Deserializer<'a>) -> Result<Self>;
}

impl Serialize for u8 {
    fn serialize(&self, serializer: &mut Serializer) -> Result<()> {
        serializer.write_u8(*self);
        Ok(())
    }
}

impl Serialize for u16 {
    fn serialize(&self, serializer: &mut Serializer) -> Result<()> {
        serializer.write_u16(*self);
        Ok(())
    }
}
impl Serialize for u32 {
    fn serialize(&self, serializer: &mut Serializer) -> Result<()> {
        serializer.write_u32(*self);
        Ok(())
    }
}
impl Serialize for u64 {
    fn serialize(&self, serializer: &mut Serializer) -> Result<()> {
        serializer.write_u64(*self);
        Ok(())
    }
}

impl Serialize for String {
    fn serialize(&self, serializer: &mut Serializer) -> Result<()> {
        serializer.write_string(self);
        Ok(())
    }
}

impl Serialize for Vec<u8> {
    fn serialize(&self, serializer: &mut Serializer) -> Result<()> {
        serializer.write_u32(self.len() as u32);
        serializer.write_bytes(self);
        Ok(())
    }
}

impl<'a> Deserialize<'a> for u8 {
    fn deserialize(deserializer: &mut Deserializer<'a>) -> Result<Self> {
        deserializer.read_u8()
    }
}

impl<'a> Deserialize<'a> for u16 {
    fn deserialize(deserializer: &mut Deserializer<'a>) -> Result<Self> {
        deserializer.read_u16()
    }
}
impl<'a> Deserialize<'a> for u32 {
    fn deserialize(deserializer: &mut Deserializer<'a>) -> Result<Self> {
        deserializer.read_u32()
    }
}
impl<'a> Deserialize<'a> for u64 {
    fn deserialize(deserializer: &mut Deserializer<'a>) -> Result<Self> {
        deserializer.read_u64()
    }
}

impl<'a> Deserialize<'a> for String {
    fn deserialize(deserializer: &mut Deserializer<'a>) -> Result<Self> {
        deserializer.read_string()
    }
}
impl<'a> Deserialize<'a> for Vec<u8> {
    fn deserialize(deserializer: &mut Deserializer<'a>) -> Result<Self> {
        let len = deserializer.read_u32()? as usize;
        let bytes = deserializer.read_bytes(len)?;

        Ok(bytes.to_vec())
    }
}

pub mod utils {
    use super::*;

    pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>> {
        let mut serializer = Serializer::new();
        value.serialize(&mut serializer)?;
        Ok(serializer.finish())
    }

    pub fn deseriaze<'a, T: Deserialize<'a>>(data: &'a [u8]) -> Result<T> {
        let mut deserializer = Deserializer::new(data);
        T::deserialize(&mut deserializer)
    }

    pub fn serialized_sized<T: Serialize>(value: &T) -> Result<usize> {
        let mut serializer = Serializer::new();
        value.serialize(&mut serializer)?;
        Ok(serializer.size())
    }
}

// Ini Bahasa RustLang
// Ini teks editor NeoVim
// Belajar Bikin Database
