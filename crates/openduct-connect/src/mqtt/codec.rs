// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! MQTT 3.1.1 packets, the few a client sends and receives. Reading checks every length against what is there
//! and against the most a packet may be before setting any memory aside, so a hostile MQTT broker can only be
//! refused, never make the reader fail another way.

use bytes::{Buf, BufMut, Bytes, BytesMut};

const CONNECT: u8 = 1;
const CONNACK: u8 = 2;
const PUBLISH: u8 = 3;
const PUBACK: u8 = 4;
const SUBSCRIBE: u8 = 8;
const SUBACK: u8 = 9;
const UNSUBACK: u8 = 11;
const PINGREQ: u8 = 12;
const PINGRESP: u8 = 13;
const DISCONNECT: u8 = 14;

/// The SUBACK return code for a refused subscription.
pub(super) const REFUSED: u8 = 0x80;

/// A message, as it travels either way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Publish {
    pub topic: String,
    pub payload: Bytes,
    pub qos: u8,
    pub retain: bool,
    pub dup: bool,
    /// 0 for QoS 0, which has none.
    pub pkid: u16,
}

/// What an MQTT broker sends a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Incoming {
    ConnAck { session_present: bool, code: u8 },
    Publish(Publish),
    PubAck(u16),
    SubAck { pkid: u16, codes: Vec<u8> },
    UnsubAck,
    PingResp,
}

/// How a client introduces itself.
#[derive(Debug, Clone)]
pub(super) struct Connect<'a> {
    pub client_id: &'a str,
    pub keep_alive_secs: u16,
    pub clean_session: bool,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
}

/// The next whole packet in `buf`, taken out of it; `None` while it has not all arrived.
pub(super) fn decode(buf: &mut BytesMut, max: usize) -> Result<Option<Incoming>, String> {
    let Some(&first) = buf.first() else {
        return Ok(None);
    };
    let mut remaining = 0usize;
    let mut header = 1;
    loop {
        let Some(&byte) = buf.get(header) else {
            return Ok(None);
        };
        remaining |= usize::from(byte & 0x7f) << (7 * (header - 1));
        header += 1;
        if byte & 0x80 == 0 {
            break;
        }
        if header > 4 {
            return Err("a packet's length is not valid".to_owned());
        }
    }
    if remaining > max {
        return Err(format!(
            "it sent a packet of {remaining} bytes; the most one can be is {} MiB",
            max / (1024 * 1024)
        ));
    }
    let total = header + remaining;
    if buf.len() < total {
        buf.reserve(total - buf.len());
        return Ok(None);
    }
    let mut packet = buf.split_to(total).freeze();
    packet.advance(header);
    parse(first >> 4, first & 0x0f, packet).map(Some)
}

fn parse(kind: u8, flags: u8, mut body: Bytes) -> Result<Incoming, String> {
    let packet = match kind {
        CONNACK => {
            let ack = take_u8(&mut body)?;
            let code = take_u8(&mut body)?;
            Incoming::ConnAck {
                session_present: ack & 1 == 1,
                code,
            }
        }
        PUBLISH => {
            let qos = (flags >> 1) & 0b11;
            if qos > 2 {
                return Err("a message has QoS 3, which does not exist".to_owned());
            }
            let topic = take_string(&mut body)?;
            let pkid = if qos > 0 { take_u16(&mut body)? } else { 0 };
            if qos > 0 && pkid == 0 {
                return Err("a message has packet id 0".to_owned());
            }
            return Ok(Incoming::Publish(Publish {
                topic,
                payload: body,
                qos,
                retain: flags & 1 == 1,
                dup: flags & 0b1000 != 0,
                pkid,
            }));
        }
        PUBACK => Incoming::PubAck(take_u16(&mut body)?),
        SUBACK => {
            let pkid = take_u16(&mut body)?;
            let codes = body.to_vec();
            body.clear();
            Incoming::SubAck { pkid, codes }
        }
        UNSUBACK => {
            take_u16(&mut body)?;
            Incoming::UnsubAck
        }
        PINGRESP => Incoming::PingResp,
        other => {
            return Err(format!(
                "it sent a packet of type {other}, which a client is not sent"
            ));
        }
    };
    if body.has_remaining() {
        return Err("a packet is longer than what it holds".to_owned());
    }
    Ok(packet)
}

fn take_u8(body: &mut Bytes) -> Result<u8, String> {
    if body.remaining() < 1 {
        return Err("a packet ends early".to_owned());
    }
    Ok(body.get_u8())
}

fn take_u16(body: &mut Bytes) -> Result<u16, String> {
    if body.remaining() < 2 {
        return Err("a packet ends early".to_owned());
    }
    Ok(body.get_u16())
}

fn take_string(body: &mut Bytes) -> Result<String, String> {
    let len = usize::from(take_u16(body)?);
    if body.remaining() < len {
        return Err("a packet ends early".to_owned());
    }
    String::from_utf8(body.split_to(len).to_vec()).map_err(|_| "a topic is not UTF-8".to_owned())
}

// ------------------------------------------------------------------------------------------------------ writing

fn finish(kind: u8, flags: u8, body: &[u8]) -> Result<Bytes, String> {
    let mut len = body.len();
    if len > 268_435_455 {
        return Err("a packet is larger than MQTT allows".to_owned());
    }
    let mut out = BytesMut::with_capacity(body.len() + 5);
    out.put_u8((kind << 4) | flags);
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.put_u8(byte);
        if len == 0 {
            break;
        }
    }
    out.put_slice(body);
    Ok(out.freeze())
}

fn put_bytes(out: &mut BytesMut, bytes: &[u8]) -> Result<(), String> {
    let len = u16::try_from(bytes.len())
        .map_err(|_| "a name in a packet is longer than 65,535 bytes".to_owned())?;
    out.put_u16(len);
    out.put_slice(bytes);
    Ok(())
}

pub(super) fn connect(c: &Connect<'_>) -> Result<Bytes, String> {
    let mut body = BytesMut::new();
    put_bytes(&mut body, b"MQTT")?;
    body.put_u8(4);
    let mut flags = 0u8;
    if c.username.is_some() {
        flags |= 0x80;
    }
    if c.password.is_some() {
        flags |= 0x40;
    }
    if c.clean_session {
        flags |= 0x02;
    }
    body.put_u8(flags);
    body.put_u16(c.keep_alive_secs);
    put_bytes(&mut body, c.client_id.as_bytes())?;
    if let Some(user) = c.username {
        put_bytes(&mut body, user.as_bytes())?;
    }
    if let Some(password) = c.password {
        put_bytes(&mut body, password.as_bytes())?;
    }
    finish(CONNECT, 0, &body)
}

pub(super) fn publish(p: &Publish) -> Result<Bytes, String> {
    let mut body = BytesMut::with_capacity(p.topic.len() + p.payload.len() + 4);
    put_bytes(&mut body, p.topic.as_bytes())?;
    if p.qos > 0 {
        body.put_u16(p.pkid);
    }
    body.put_slice(&p.payload);
    let flags = (u8::from(p.dup) << 3) | ((p.qos & 0b11) << 1) | u8::from(p.retain);
    finish(PUBLISH, flags, &body)
}

pub(super) fn puback(pkid: u16) -> Bytes {
    Bytes::copy_from_slice(&[PUBACK << 4, 2, (pkid >> 8) as u8, pkid as u8])
}

pub(super) fn subscribe(pkid: u16, filters: &[(String, u8)]) -> Result<Bytes, String> {
    let mut body = BytesMut::new();
    body.put_u16(pkid);
    for (filter, qos) in filters {
        put_bytes(&mut body, filter.as_bytes())?;
        body.put_u8(*qos);
    }
    finish(SUBSCRIBE, 0b0010, &body)
}

pub(super) fn pingreq() -> Bytes {
    Bytes::from_static(&[PINGREQ << 4, 0])
}

pub(super) fn disconnect() -> Bytes {
    Bytes::from_static(&[DISCONNECT << 4, 0])
}

/// The hostile-input tests' door (see `crate::hostile`): `bytes` read as packets from a broker, as the session reads
/// them: whole packets taken off the front while they have arrived, at most `max` bytes each.
pub(crate) fn replay(bytes: &[u8], max: usize) {
    let mut buf = BytesMut::from(bytes);
    while let Ok(Some(_)) = decode(&mut buf, max) {}
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn decoded(bytes: &[u8]) -> Result<Option<Incoming>, String> {
        decode(&mut BytesMut::from(bytes), 1024)
    }

    #[test]
    fn packets_from_an_mqtt_broker_are_read() {
        assert_eq!(
            decoded(&[0x20, 2, 1, 0]).unwrap(),
            Some(Incoming::ConnAck {
                session_present: true,
                code: 0
            })
        );
        assert_eq!(
            decoded(&[0x40, 2, 0x12, 0x34]).unwrap(),
            Some(Incoming::PubAck(0x1234))
        );
        assert_eq!(
            decoded(&[0x90, 4, 0, 7, 1, 0x80]).unwrap(),
            Some(Incoming::SubAck {
                pkid: 7,
                codes: vec![1, REFUSED]
            })
        );
        assert_eq!(decoded(&[0xd0, 0]).unwrap(), Some(Incoming::PingResp));
        // PUBLISH, QoS 1, retained, duplicate: topic "a/b", packet id 9, payload "hi".
        let got = decoded(&[0x3b, 9, 0, 3, b'a', b'/', b'b', 0, 9, b'h', b'i']).unwrap();
        assert_eq!(
            got,
            Some(Incoming::Publish(Publish {
                topic: "a/b".to_owned(),
                payload: Bytes::from_static(b"hi"),
                qos: 1,
                retain: true,
                dup: true,
                pkid: 9
            }))
        );
    }

    #[test]
    fn a_packet_is_read_only_once_it_has_all_arrived_and_the_next_waits_its_turn() {
        let mut buf = BytesMut::from(&[0x40, 2, 0][..]);
        assert_eq!(decode(&mut buf, 1024).unwrap(), None);
        buf.put_slice(&[5, 0xd0, 0]);
        assert_eq!(decode(&mut buf, 1024).unwrap(), Some(Incoming::PubAck(5)));
        assert_eq!(decode(&mut buf, 1024).unwrap(), Some(Incoming::PingResp));
        assert!(buf.is_empty());
    }

    #[test]
    fn what_a_client_sends_is_written_to_the_standard() {
        let c = connect(&Connect {
            client_id: "dev",
            keep_alive_secs: 30,
            clean_session: false,
            username: Some("u"),
            password: Some("p"),
        })
        .unwrap();
        assert_eq!(
            &c[..],
            &[
                0x10, 21, 0, 4, b'M', b'Q', b'T', b'T', 4, 0xc0, 0, 30, 0, 3, b'd', b'e', b'v', 0, 1, b'u',
                0, 1, b'p'
            ]
        );
        let s = subscribe(1, &[("a/+".to_owned(), 1)]).unwrap();
        assert_eq!(&s[..], &[0x82, 8, 0, 1, 0, 3, b'a', b'/', b'+', 1]);
        assert_eq!(&puback(0x0102)[..], &[0x40, 2, 1, 2]);
        let p = Publish {
            topic: "t".to_owned(),
            payload: Bytes::from(vec![7; 200]),
            qos: 1,
            retain: false,
            dup: false,
            pkid: 3,
        };
        let bytes = publish(&p).unwrap();
        // 200 + 3 + 2 = 205 bytes: two length bytes.
        assert_eq!(&bytes[..3], &[0x32, 205 % 128 + 128, 1]);
        // What is written reads back the same.
        let mut buf = BytesMut::from(&bytes[..]);
        assert_eq!(decode(&mut buf, 1024).unwrap(), Some(Incoming::Publish(p)));
    }

    #[test]
    fn hostile_packets_are_refused() {
        // A length longer than four bytes, one past the most allowed, and one claiming 256 MiB.
        assert!(decoded(&[0x30, 0xff, 0xff, 0xff, 0xff, 1]).is_err());
        assert!(decoded(&[0x30, 0x81, 0x08]).unwrap_err().contains("1025 bytes"));
        assert!(decoded(&[0x30, 0xff, 0xff, 0xff, 0x7f]).is_err());
        // A topic longer than its packet, QoS 3, packet id 0, a CONNECT a client is never sent, trailing bytes.
        assert!(decoded(&[0x30, 3, 0, 9, b'a']).is_err());
        assert!(decoded(&[0x36, 4, 0, 1, b'a', 0]).is_err());
        assert!(decoded(&[0x32, 5, 0, 1, b'a', 0, 0]).is_err());
        assert!(decoded(&[0x10, 0]).is_err());
        assert!(decoded(&[0x40, 3, 0, 1, 9]).is_err());
        assert!(
            decoded(&[0x30, 4, 0, 2, 0xff, 0xfe])
                .unwrap_err()
                .contains("UTF-8")
        );
    }

    #[test]
    fn random_bytes_never_break_the_reader() {
        // A small, repeatable generator: every input is refused or read, none panics or loops.
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..200_000 {
            let len = (next() % 24) as usize;
            let mut bytes: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            if let Some(first) = bytes.first_mut() {
                // Mostly packet types a client is sent, so the parsing behind them is reached.
                *first = (*first & 0x0f) | ([2u8, 3, 4, 9, 11, 13][(next() % 6) as usize] << 4);
            }
            let mut buf = BytesMut::from(&bytes[..]);
            for _ in 0..len + 1 {
                match decode(&mut buf, 64) {
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }
}
