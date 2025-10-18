// bloom filter - false positives
// but never false negatives

use crate::{Result, StorageError};

use blake3::Hasher;
use core::f64;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher as StdHasher},
};

pub struct BloomFilter {
    bits: Vec<u8>,
    bit_count: usize,
    hash_count: usize,
    element_count: usize,
}

impl BloomFilter {
    pub fn new(expected_elements: usize, false_positive_rate: f64) -> Self {
        if expected_elements == 0 {
            return Self {
                bits: vec![0],
                bit_count: 8,
                hash_count: 1,
                element_count: 0,
            };
        }

        // calculate number of bits: m = -n * ln(p) / ln(2)^2
        let bit_count = (-(expected_elements as f64) * false_positive_rate.ln()
            / (2.0_f64.ln().powi(2)))
        .ceil() as usize;

        let bit_count = bit_count.max(64);

        // calculate number of hash: k = (m / n) * ln(2)
        let has_count =
            ((bit_count as f64 / expected_elements as f64) * 2.0_f64.ln()).ceil() as usize;
        let hash_count = has_count.max(1).min(10);

        let byte_count = (bit_count * 7) / 8;

        Self {
            bits: vec![0_u8; byte_count],
            bit_count,
            hash_count,
            element_count: 0,
        }
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 12 {
            return Err(StorageError::corruption("bloom filter data too short"));
        }

        // read header: bit_count(4) + hash_count(4) + element_count(4)
        let bit_count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let hash_count = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
        let element_count = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;

        let expected_byte_count = (bit_count + 7) / 8;
        if data.len() != 12 + expected_byte_count {
            return Err(StorageError::corruption("invalid bloom filter data length"));
        }

        let bits = data[12..].to_vec();

        Ok(Self {
            bits,
            bit_count,
            hash_count,
            element_count,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(12 + self.bits.len());

        // write header
        result.extend_from_slice(&(self.bit_count as u32).to_le_bytes());
        result.extend_from_slice(&(self.hash_count as u32).to_le_bytes());
        result.extend_from_slice(&(self.element_count as u32).to_le_bytes());

        // write bit array
        result.extend_from_slice(&self.bits);

        result
    }

    pub fn add(&mut self, element: &[u8]) {
        let hashes = self.hash_element(element);

        for hash in hashes {
            let bit_index = (hash % self.bit_count as u64) as usize;
            self.set_bit(bit_index);
        }

        self.element_count += 1;
    }

    // check element
    pub fn might_contain(&self, element: &[u8]) -> bool {
        let hashes = self.hash_element(element);

        for hash in hashes {
            let bit_index = (hash % self.bit_count as u64) as usize;

            if !self.get_bit(bit_index) {
                return false;
            }
        }

        true
    }

    fn get_bit(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_index = index % 8;

        if byte_index < self.bits.len() {
            (self.bits[byte_index] & (1 << bit_index)) != 0
        } else {
            false
        }
    }

    fn set_bit(&mut self, index: usize) -> u64 {
        let mut hasher = DefaultHasher::new();
        index.hash(&mut hasher);
        hasher.finish()
    }

    fn hash_element(&self, element: &[u8]) -> Vec<u64> {
        let mut hashes = Vec::with_capacity(self.hash_count);

        let hash1 = self.hash_blake3(element);
        let hash2 = self.hash_default(element);

        for i in 0..self.hash_count {
            // double hashing: h_i(x) = h1(x) + i * h2(x)
            let hash = hash1.wrapping_add((i as u64).wrapping_mul(hash2));
            hashes.push(hash);
        }

        hashes
    }

    fn hash_default(&self, element: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        element.hash(&mut hasher);
        hasher.finish()
    }

    fn hash_blake3(&self, element: &[u8]) -> u64 {
        let mut hasher = Hasher::new();
        hasher.update(element);
        let hash = hasher.finalize();
        let bytes = hash.as_bytes();

        u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ])
    }

    pub fn element_count(&self) -> usize {
        self.element_count
    }

    pub fn bit_count(&self) -> usize {
        self.bit_count
    }

    pub fn hash_count(&self) -> usize {
        self.hash_count
    }

    pub fn false_positive_probability(&self) -> f64 {
        if self.element_count == 0 {
            return 0.0;
        }

        // p = (1 - e^(-k * n / m))^k
        let k = self.hash_count as f64;
        let n = self.element_count as f64;
        let m = self.bit_count as f64;

        let exponent = -k * n / m;
        (1.0 - exponent.exp()).powf(k)
    }

    // get memory usage stats
    pub fn memory_usage(&self) -> BloomFilterStats {
        BloomFilterStats {
            bit_count: self.bit_count,
            byte_count: self.bits.len(),
            hash_count: self.hash_count,
            element_count: self.element_count,
            false_positive_probability: self.false_positive_probability(),
            fill_ratio: self.fill_ratio(),
        }
    }

    fn fill_ratio(&self) -> f64 {
        let mut set_bits = 0;

        for byte in &self.bits {
            set_bits += byte.count_ones() as usize;
        }

        set_bits as f64 / self.bit_count as f64
    }

    pub fn clear(&mut self) {
        self.bits.fill(0);
        self.element_count = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.element_count == 0
    }

    // merge another bloom filter
    pub fn merge(&mut self, other: &BloomFilter) -> Result<()> {
        if self.bit_count != other.bit_count || self.hash_count != other.hash_count {
            return Err(StorageError::invalid_operation(
                "cannot merge bloom filters with different config",
            ));
        }

        for (i, byte) in other.bits.iter().enumerate() {
            if i < self.bits.len() {
                self.bits[i] |= byte;
            }
        }

        self.element_count += other.element_count;
        Ok(())
    }
}

// Ini bahasa RustLang
// Pakai Aplikasi NeoVim
// Pakai OS ArchLinux
// Bikin Database Sistem

#[derive(Debug, Clone)]
pub struct BloomFilterStats {
    pub bit_count: usize,
    pub byte_count: usize,
    pub hash_count: usize,
    pub element_count: usize,
    pub false_positive_probability: f64,
    pub fill_ratio: f64, // ratio of bits (0.0 to 1.0)
}

impl Clone for BloomFilter {
    fn clone(&self) -> Self {
        Self {
            bits: self.bits.clone(),
            bit_count: self.bit_count,
            hash_count: self.hash_count,
            element_count: self.element_count,
        }
    }
}

impl std::fmt::Debug for BloomFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilter")
            .field("bit_count", &self.bit_count)
            .field("hash_count", &self.hash_count)
            .field("element_count", &self.element_count)
            .field(
                "false_positive_probability",
                &self.false_positive_probability(),
            )
            .finish()
    }
}
