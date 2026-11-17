// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A Modbus TCP client for the four reads: coils, discrete inputs, holding registers and input registers. One
//! request at a time on a connection; every answer is checked (its transaction, length, function and byte count)
//! before it is used, so a device or gateway that sends something else is refused, never misread.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// The most registers one read asks for, and the most coils or inputs.
pub(super) const MAX_REGISTERS: u16 = 125;
pub(super) const MAX_BITS: u16 = 2_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum Table {
    Coil,
    Discrete,
    Holding,
    Input,
}

impl Table {
    pub(super) fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "coil" | "coils" => Self::Coil,
            "discrete" | "discrete_input" | "discrete_inputs" => Self::Discrete,
            "holding" | "holding_register" | "holding_registers" => Self::Holding,
            "input" | "input_register" | "input_registers" => Self::Input,
            _ => return None,
        })
    }

    pub(super) fn bits(self) -> bool {
        matches!(self, Self::Coil | Self::Discrete)
    }

    fn function(self) -> u8 {
        match self {
            Self::Coil => 1,
            Self::Discrete => 2,
            Self::Holding => 3,
            Self::Input => 4,
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Coil => "coil",
            Self::Discrete => "discrete input",
            Self::Holding => "holding register",
            Self::Input => "input register",
        }
    }
}

/// What a read gave: bits for coils and discrete inputs, 16-bit words for registers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Data {
    Bits(Vec<bool>),
    Words(Vec<u16>),
}

#[derive(Debug)]
pub(super) enum Failure {
    /// Asking again will not help: an address the device does not have, say.
    Lasting(String),
    /// Asking again may: a dropped connection or a busy device.
    Passing(String),
}

#[derive(Debug)]
pub(super) struct Client {
    stream: TcpStream,
    unit: u8,
    transaction: u16,
    wait: Duration,
}

impl Client {
    pub(super) async fn connect(host: &str, port: u16, unit: u8, wait: Duration) -> Result<Self, Failure> {
        let stream = timeout(wait, TcpStream::connect((host, port)))
            .await
            .map_err(|_| Failure::Passing(format!("the device did not answer within {wait:?}")))?
            .map_err(|e| Failure::Passing(format!("cannot connect to the device: {e}")))?;
        let _ = stream.set_nodelay(true);
        Ok(Self {
            stream,
            unit,
            transaction: 0,
            wait,
        })
    }

    /// `count` values of `table` from `address`.
    pub(super) async fn read(&mut self, table: Table, address: u16, count: u16) -> Result<Data, Failure> {
        let what = format!("{} {address}", table.name());
        let limit = if table.bits() { MAX_BITS } else { MAX_REGISTERS };
        if count == 0 || count > limit || u32::from(address) + u32::from(count) > 65_536 {
            return Err(Failure::Lasting(format!(
                "cannot read {count} from {what}: one read is 1 to {limit}, within addresses 0 to 65535"
            )));
        }
        self.transaction = self.transaction.wrapping_add(1);
        let [t1, t2] = self.transaction.to_be_bytes();
        let [a1, a2] = address.to_be_bytes();
        let [c1, c2] = count.to_be_bytes();
        let function = table.function();
        let request = [t1, t2, 0, 0, 0, 6, self.unit, function, a1, a2, c1, c2];
        let wait = self.wait;
        let answer = timeout(wait, async {
            self.stream.write_all(&request).await?;
            let mut header = [0u8; 7];
            self.stream.read_exact(&mut header).await?;
            let [h1, h2, p1, p2, l1, l2, _unit] = header;
            if [p1, p2] != [0, 0] {
                return Ok(Err(Failure::Lasting(
                    "the device's answer is not Modbus TCP".to_owned(),
                )));
            }
            if [h1, h2] != [t1, t2] {
                // An answer to an earlier request: a new connection starts clean.
                return Ok(Err(Failure::Passing(
                    "the device answered an earlier request".to_owned(),
                )));
            }
            // The length counts the unit id and what follows: 2 to 254 bytes (an answer is at most 260).
            let length = usize::from(u16::from_be_bytes([l1, l2]));
            if !(2..=254).contains(&length) {
                return Ok(Err(Failure::Lasting(format!(
                    "the device's answer claims {length} bytes, which Modbus does not allow"
                ))));
            }
            let mut pdu = vec![0u8; length - 1];
            self.stream.read_exact(&mut pdu).await?;
            Ok::<_, std::io::Error>(Ok(pdu))
        })
        .await
        .map_err(|_| {
            Failure::Passing(format!(
                "the device did not answer reading {what} within {wait:?}"
            ))
        })?
        .map_err(|e| Failure::Passing(format!("the connection to the device broke: {e}")))??;
        parse(&answer, function, table, count, &what)
    }
}

fn parse(pdu: &[u8], function: u8, table: Table, count: u16, what: &str) -> Result<Data, Failure> {
    match pdu {
        [f, code] if *f == function | 0x80 => Err(exception(*code, what)),
        [f, bytes, data @ ..] if *f == function => {
            let expected = if table.bits() {
                usize::from(count).div_ceil(8)
            } else {
                usize::from(count) * 2
            };
            if usize::from(*bytes) != expected {
                return Err(Failure::Lasting(format!(
                    "the device answered reading {what} with {bytes} bytes, where {expected} were asked for"
                )));
            }
            if data.len() != expected {
                return Err(Failure::Lasting(format!(
                    "the device's answer reading {what} holds {} bytes, where it says {bytes}",
                    data.len()
                )));
            }
            Ok(if table.bits() {
                Data::Bits(
                    (0..usize::from(count))
                        .map(|i| data.get(i / 8).is_some_and(|b| b >> (i % 8) & 1 == 1))
                        .collect(),
                )
            } else {
                Data::Words(
                    data.as_chunks::<2>()
                        .0
                        .iter()
                        .map(|w| u16::from_be_bytes(*w))
                        .collect(),
                )
            })
        }
        _ => Err(Failure::Lasting(format!(
            "the device answered reading {what} with something else"
        ))),
    }
}

/// A Modbus exception, in words; whether asking again may help.
fn exception(code: u8, what: &str) -> Failure {
    let why = match code {
        1 => "it does not support this read (illegal function)",
        2 => "it has no such address (illegal data address)",
        3 => "the count is not allowed (illegal data value)",
        4 => "the device failed (server device failure)",
        5 | 6 => "the device is busy",
        10 => "the gateway has no path to the device (gateway path unavailable)",
        11 => "the device behind the gateway did not answer (gateway target failed to respond)",
        _ => "an exception the standard does not name",
    };
    let message = format!("the device refused reading {what}: {why} (code {code})");
    if matches!(code, 4..=6 | 10 | 11) {
        Failure::Passing(message)
    } else {
        Failure::Lasting(message)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn answers_are_read_and_checked() {
        assert_eq!(
            parse(&[3, 4, 0, 215, 0x40, 0x88], 3, Table::Holding, 2, "x").unwrap(),
            Data::Words(vec![215, 0x4088])
        );
        // Coils come eight to a byte, the first in the lowest bit.
        assert_eq!(
            parse(&[1, 2, 0b0000_0101, 0b1], 1, Table::Coil, 9, "x").unwrap(),
            Data::Bits(vec![true, false, true, false, false, false, false, false, true])
        );
        assert!(
            matches!(parse(&[0x83, 2], 3, Table::Holding, 1, "holding register 900"), Err(Failure::Lasting(m)) if m.contains("no such address"))
        );
        assert!(matches!(
            parse(&[0x83, 6], 3, Table::Holding, 1, "x"),
            Err(Failure::Passing(_))
        ));
        // A short answer, a byte count that does not match, another function, nothing at all.
        assert!(parse(&[3, 4, 0, 1], 3, Table::Holding, 2, "x").is_err());
        assert!(parse(&[3, 2, 0, 1, 0, 2], 3, Table::Holding, 2, "x").is_err());
        assert!(parse(&[4, 2, 0, 1], 3, Table::Holding, 1, "x").is_err());
        assert!(parse(&[], 3, Table::Holding, 1, "x").is_err());
    }
}
