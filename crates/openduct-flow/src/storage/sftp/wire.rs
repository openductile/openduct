// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! SSH's data types (RFC 4251 section 5): bytes, whole numbers, strings, name lists and big numbers, written and
//! read - each read bounded by what is there, so a short or lying message is an error, never a panic.

/// A message being written.
#[derive(Debug, Default, Clone)]
pub(crate) struct Out(pub Vec<u8>);

impl Out {
    pub(crate) fn new(kind: u8) -> Self {
        Self(vec![kind])
    }

    pub(crate) fn byte(mut self, b: u8) -> Self {
        self.0.push(b);
        self
    }

    pub(crate) fn bool(self, b: bool) -> Self {
        self.byte(u8::from(b))
    }

    pub(crate) fn u32(mut self, n: u32) -> Self {
        self.0.extend_from_slice(&n.to_be_bytes());
        self
    }

    pub(crate) fn u64(mut self, n: u64) -> Self {
        self.0.extend_from_slice(&n.to_be_bytes());
        self
    }

    pub(crate) fn string(mut self, s: &[u8]) -> Self {
        self.0
            .extend_from_slice(&u32::try_from(s.len()).unwrap_or(u32::MAX).to_be_bytes());
        self.0.extend_from_slice(s);
        self
    }

    pub(crate) fn text(self, s: &str) -> Self {
        self.string(s.as_bytes())
    }

    /// A big number from its unsigned big-endian bytes, as SSH writes one (`mpint`).
    pub(crate) fn mpint(self, bytes: &[u8]) -> Self {
        let trimmed = trim_zeros(bytes);
        if trimmed.first().is_some_and(|b| b & 0x80 != 0) {
            let mut padded = Vec::with_capacity(trimmed.len() + 1);
            padded.push(0);
            padded.extend_from_slice(trimmed);
            self.string(&padded)
        } else {
            self.string(trimmed)
        }
    }

    pub(crate) fn raw(mut self, bytes: &[u8]) -> Self {
        self.0.extend_from_slice(bytes);
        self
    }
}

fn trim_zeros(bytes: &[u8]) -> &[u8] {
    let start = bytes.iter().position(|b| *b != 0).unwrap_or(bytes.len());
    bytes.get(start..).unwrap_or_default()
}

/// A message being read.
#[derive(Debug, Clone)]
pub(crate) struct In<'a> {
    bytes: &'a [u8],
    at: usize,
}

const SHORT: &str = "the server sent a message cut short";

impl<'a> In<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let out = self.bytes.get(self.at..self.at.saturating_add(n)).ok_or(SHORT)?;
        self.at += n;
        Ok(out)
    }

    pub(crate) fn byte(&mut self) -> Result<u8, String> {
        self.take(1)?.first().copied().ok_or_else(|| SHORT.to_owned())
    }

    pub(crate) fn bool(&mut self) -> Result<bool, String> {
        Ok(self.byte()? != 0)
    }

    pub(crate) fn u32(&mut self) -> Result<u32, String> {
        let b: [u8; 4] = self.take(4)?.try_into().map_err(|_| SHORT.to_owned())?;
        Ok(u32::from_be_bytes(b))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, String> {
        let b: [u8; 8] = self.take(8)?.try_into().map_err(|_| SHORT.to_owned())?;
        Ok(u64::from_be_bytes(b))
    }

    pub(crate) fn string(&mut self) -> Result<&'a [u8], String> {
        let n = usize::try_from(self.u32()?).map_err(|_| SHORT.to_owned())?;
        self.take(n)
    }

    pub(crate) fn text(&mut self) -> Result<String, String> {
        Ok(String::from_utf8_lossy(self.string()?).into_owned())
    }

    /// A big number's unsigned big-endian bytes (its leading zero, if any, gone).
    pub(crate) fn mpint(&mut self) -> Result<&'a [u8], String> {
        Ok(trim_zeros(self.string()?))
    }

    pub(crate) fn rest(&self) -> &'a [u8] {
        self.bytes.get(self.at..).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_written_and_read_as_ssh_writes_them() {
        // RFC 4251's own examples of mpints.
        assert_eq!(Out::default().mpint(&[]).0, [0, 0, 0, 0]);
        assert_eq!(
            Out::default()
                .mpint(&[0x09, 0xa3, 0x78, 0xf9, 0xb2, 0xe3, 0x32, 0xa7])
                .0,
            [0, 0, 0, 8, 0x09, 0xa3, 0x78, 0xf9, 0xb2, 0xe3, 0x32, 0xa7]
        );
        assert_eq!(Out::default().mpint(&[0x80]).0, [0, 0, 0, 2, 0, 0x80]);
        assert_eq!(Out::default().mpint(&[0, 0, 0x7f]).0, [0, 0, 0, 1, 0x7f]);
        let m = Out::new(5).u32(7).text("ssh-userauth").bool(true).u64(9).0;
        let mut r = In::new(&m);
        assert_eq!(r.byte().unwrap(), 5);
        assert_eq!(r.u32().unwrap(), 7);
        assert_eq!(r.text().unwrap(), "ssh-userauth");
        assert!(r.bool().unwrap());
        assert_eq!(r.u64().unwrap(), 9);
        assert!(r.byte().is_err(), "nothing more");
        // A string that says it is longer than the message is refused.
        assert!(In::new(&[0, 0, 0, 9, 1]).string().is_err());
        assert!(In::new(&[0xff, 0xff, 0xff, 0xff]).string().is_err());
    }
}
