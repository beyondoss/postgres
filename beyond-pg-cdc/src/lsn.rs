use std::fmt;
use std::str::FromStr;

/// A Postgres Log Sequence Number (LSN). Wire format is a big-endian u64;
/// canonical text format is `"{hi:X}/{lo:08X}"` (e.g. `"1/2345678"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Lsn(u64::from_be_bytes(bytes))
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hi = (self.0 >> 32) as u32;
        let lo = (self.0 & 0xFFFF_FFFF) as u32;
        write!(f, "{hi:X}/{lo:08X}")
    }
}

impl FromStr for Lsn {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi, lo) = s
            .split_once('/')
            .ok_or_else(|| format!("invalid LSN '{s}': missing '/'"))?;
        let hi = u32::from_str_radix(hi, 16)
            .map_err(|e| format!("invalid LSN high half '{hi}': {e}"))?;
        let lo =
            u32::from_str_radix(lo, 16).map_err(|e| format!("invalid LSN low half '{lo}': {e}"))?;
        Ok(Lsn(((hi as u64) << 32) | (lo as u64)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let lsn = Lsn(0x0000_0001_2345_6780);
        assert_eq!(lsn.to_string(), "1/23456780");
        assert_eq!("1/23456780".parse::<Lsn>().unwrap(), lsn);
    }

    #[test]
    fn zero() {
        assert_eq!(Lsn::ZERO.to_string(), "0/00000000");
        assert_eq!("0/00000000".parse::<Lsn>().unwrap(), Lsn::ZERO);
    }

    #[test]
    fn be_bytes() {
        let lsn = Lsn(0x0102_0304_0506_0708);
        assert_eq!(lsn.to_be_bytes(), [1, 2, 3, 4, 5, 6, 7, 8]);
    }
}
