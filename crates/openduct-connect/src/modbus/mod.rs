// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: modbus:` - machines, meters and PLCs over Modbus TCP. Named values are read on a schedule and each read
//! becomes one message, a JSON object of the values by name.
//!
//! ```yaml
//! from:
//!   modbus:
//!     url: modbus://10.0.4.20:502
//!     device: press-07
//!     every: 5s
//!     read:
//!       temperature: { table: holding, address: 100, type: i16, scale: 0.1 }
//!       pressure:    { table: holding, address: 101, type: f32 }
//!       running:     { table: coil, address: 3 }
//! ```

mod client;

use std::str::FromStr;
use std::time::Duration;

use openduct_pipeline::Connector;
use rust_decimal::Decimal;
use tokio::time::{Instant, sleep_until};

use self::client::{Client, Data, Failure, MAX_BITS, MAX_REGISTERS, Table};
use crate::settings::{Limits, Settings};
use crate::web::ATTEMPTS;
use crate::{SourceBatch, SourceMessage};

const SETTINGS: &[&str] = &[
    "url",
    "unit",
    "device",
    "every",
    "timeout",
    "word_order",
    "changes_only",
    "read",
];
const POINT_SETTINGS: &[&str] = &["table", "address", "type", "bit", "scale"];
/// The most values one source reads.
const MAX_POINTS: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Bool,
    U16,
    I16,
    U32,
    I32,
    F32,
    U64,
    I64,
    F64,
}

impl Kind {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "bool" => Self::Bool,
            "u16" | "uint16" => Self::U16,
            "i16" | "int16" => Self::I16,
            "u32" | "uint32" => Self::U32,
            "i32" | "int32" => Self::I32,
            "f32" | "float32" | "float" => Self::F32,
            "u64" | "uint64" => Self::U64,
            "i64" | "int64" => Self::I64,
            "f64" | "float64" | "double" => Self::F64,
            _ => return None,
        })
    }

    /// The registers a value takes.
    fn words(self) -> u16 {
        match self {
            Self::Bool | Self::U16 | Self::I16 => 1,
            Self::U32 | Self::I32 | Self::F32 => 2,
            Self::U64 | Self::I64 | Self::F64 => 4,
        }
    }
}

/// One named value: where it is and how to read it.
#[derive(Debug, Clone)]
struct Point {
    name: String,
    table: Table,
    address: u16,
    kind: Kind,
    /// A single bit of a register, for status words.
    bit: Option<u8>,
    scale: Option<Decimal>,
}

impl Point {
    fn len(&self) -> u16 {
        if self.table.bits() { 1 } else { self.kind.words() }
    }
}

/// One request on the wire, and the values it covers.
#[derive(Debug, Clone)]
struct Request {
    table: Table,
    address: u16,
    count: u16,
    points: Vec<usize>,
}

#[derive(Debug)]
pub struct ModbusSource {
    host: String,
    port: u16,
    unit: u8,
    device: String,
    every: Duration,
    wait: Duration,
    words_little_first: bool,
    changes_only: bool,
    points: Vec<Point>,
    requests: Vec<Request>,
    client: Option<Client>,
    last: Option<Vec<u8>>,
    due: Option<Instant>,
}

fn endpoint(url: &str, at: &str) -> Result<(String, u16), String> {
    let rest = url
        .strip_prefix("modbus://")
        .or_else(|| url.strip_prefix("modbus+tcp://"))
        .or_else(|| url.strip_prefix("tcp://"))
        .ok_or_else(|| format!("{at}: is modbus://host:port"))?
        .trim_end_matches('/');
    if rest.contains(['/', '@', '?', '#']) || rest.is_empty() {
        return Err(format!("{at}: is modbus://host:port"));
    }
    match rest.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() => Ok((
            host.to_owned(),
            port.parse()
                .map_err(|_| format!("{at}: {port:?} is not a port"))?,
        )),
        Some(_) => Err(format!("{at}: names no host")),
        None => Ok((rest.to_owned(), 502)),
    }
}

fn point(name: &str, items: &[(String, String)], at: &str) -> Result<Point, String> {
    if let Some((unknown, _)) = items.iter().find(|(k, _)| !POINT_SETTINGS.contains(&k.as_str())) {
        return Err(format!(
            "{at}: {unknown:?} is not a setting: the settings are {}",
            POINT_SETTINGS.join(", ")
        ));
    }
    let get = |k: &str| items.iter().find(|(n, _)| n == k).map(|(_, v)| v.trim());
    let table = match get("table") {
        None => Table::Holding,
        Some(t) => Table::parse(t)
            .ok_or_else(|| format!("{at}.table: is holding, input, coil or discrete, not {t:?}"))?,
    };
    let address: u16 = get("address")
        .ok_or_else(|| format!("{at}.address: is required (the address in the request, from 0)"))?
        .parse()
        .map_err(|_| format!("{at}.address: is a whole number from 0 to 65535"))?;
    let bit = get("bit")
        .map(|b| {
            b.parse::<u8>()
                .ok()
                .filter(|b| *b < 16)
                .ok_or_else(|| format!("{at}.bit: is 0 to 15"))
        })
        .transpose()?;
    let kind = match get("type") {
        Some(k) => Kind::parse(k).ok_or_else(|| {
            format!("{at}.type: is bool, u16, i16, u32, i32, f32, u64, i64 or f64, not {k:?}")
        })?,
        None if table.bits() || bit.is_some() => Kind::Bool,
        None => Kind::U16,
    };
    if table.bits() && kind != Kind::Bool {
        return Err(format!("{at}.type: a {} is a bool", table.name()));
    }
    if table.bits() && bit.is_some() {
        return Err(format!(
            "{at}.bit: is for a bit of a register; a {} is one already",
            table.name()
        ));
    }
    if !table.bits() && (kind == Kind::Bool) != bit.is_some() {
        return Err(format!(
            "{at}: a bool from a register names its bit, as bit: 0 to 15"
        ));
    }
    let scale = get("scale")
        .map(|s| {
            Decimal::from_str(s)
                .ok()
                .filter(|_| kind != Kind::Bool)
                .ok_or_else(|| format!("{at}.scale: is a number to multiply a number by, such as 0.1"))
        })
        .transpose()?;
    let p = Point {
        name: name.to_owned(),
        table,
        address,
        kind,
        bit,
        scale,
    };
    if u32::from(address) + u32::from(p.len()) > 65_536 {
        return Err(format!("{at}: runs past address 65535"));
    }
    Ok(p)
}

/// The requests that read every point: neighbouring values of a table are read together, never across a gap, as
/// a device can refuse an address it does not have.
fn plan(points: &[Point]) -> Vec<Request> {
    let mut order: Vec<(usize, &Point)> = points.iter().enumerate().collect();
    order.sort_by_key(|(_, p)| (p.table, p.address));
    let mut out: Vec<Request> = Vec::new();
    for (i, p) in order {
        let end = u32::from(p.address) + u32::from(p.len());
        let limit = u32::from(if p.table.bits() { MAX_BITS } else { MAX_REGISTERS });
        if let Some(last) = out.last_mut() {
            let last_end = u32::from(last.address) + u32::from(last.count);
            let span = end.max(last_end) - u32::from(last.address);
            if last.table == p.table && u32::from(p.address) <= last_end && span <= limit {
                last.count = u16::try_from(span).unwrap_or(last.count);
                last.points.push(i);
                continue;
            }
        }
        out.push(Request {
            table: p.table,
            address: p.address,
            count: p.len(),
            points: vec![i],
        });
    }
    out
}

impl ModbusSource {
    pub(crate) fn build(connector: &Connector, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, SETTINGS, limits)?;
        let (host, port) = endpoint(&s.required("url")?, &s.path("url"))?;
        s.reach(&s.path("url"), &host, port)?;
        let unit = u8::try_from(s.number("unit", 1, 0..=255)?).unwrap_or(1);
        let every = s.duration("every", Duration::from_secs(5))?;
        if every < Duration::from_millis(100) || every > Duration::from_secs(86_400) {
            return Err(format!("{}: is between 100ms and 1d", s.path("every")));
        }
        let wait = s.duration("timeout", Duration::from_secs(3))?;
        if wait < Duration::from_millis(100) || wait > Duration::from_secs(60) {
            return Err(format!("{}: is between 100ms and 60s", s.path("timeout")));
        }
        let mut points = Vec::new();
        for (name, items) in s.sections("read")? {
            points.push(point(&name, &items, &format!("{}.{name}", s.path("read")))?);
        }
        if points.is_empty() {
            return Err(format!(
                "{}: names the values to read, such as temperature: {{ table: holding, address: 100 }}",
                s.path("read")
            ));
        }
        if points.len() > MAX_POINTS {
            return Err(format!("{}: reads at most {MAX_POINTS} values", s.path("read")));
        }
        let requests = plan(&points);
        Ok(Self {
            device: s
                .text("device")?
                .unwrap_or_else(|| format!("{host}:{port}/{unit}")),
            host,
            port,
            unit,
            every,
            wait,
            words_little_first: s.choice("word_order", &["big", "little"])? == "little",
            changes_only: s.boolean("changes_only", false)?,
            points,
            requests,
            client: None,
            last: None,
            due: None,
        })
    }

    pub(crate) fn describe(&self) -> String {
        format!(
            "modbus://{}:{} unit {}, {} values in {} reads every {:?}",
            self.host,
            self.port,
            self.unit,
            self.points.len(),
            self.requests.len(),
            self.every
        )
    }

    pub(crate) async fn next(&mut self) -> Result<SourceBatch, String> {
        if let Some(due) = self.due {
            sleep_until(due).await;
        }
        self.due = Some(Instant::now() + self.every);
        let value = self.poll().await?;
        if self.changes_only && self.last.as_deref() == Some(value.as_slice()) {
            return Ok(SourceBatch::default());
        }
        self.last = Some(value.clone());
        Ok(SourceBatch {
            messages: vec![SourceMessage {
                key: Some(self.device.clone().into_bytes()),
                value: Some(value),
                headers: vec![("modbus-device".to_owned(), self.device.clone().into_bytes())],
                ack: None,
            }],
            position: None,
        })
    }

    /// Every value, asking again after a failure that passes: up to [`ATTEMPTS`] in a row.
    async fn poll(&mut self) -> Result<Vec<u8>, String> {
        let mut failures = 0u32;
        loop {
            match self.read_all().await {
                Ok(value) => return Ok(value),
                Err(Failure::Lasting(reason)) => return Err(reason),
                Err(Failure::Passing(reason)) => {
                    self.client = None;
                    failures += 1;
                    if failures >= ATTEMPTS {
                        return Err(reason);
                    }
                    tracing::warn!(%reason, "Modbus read failed; trying again");
                    let wait = Duration::from_millis(500) * 2u32.saturating_pow(failures - 1);
                    tokio::time::sleep(wait.min(Duration::from_secs(10))).await;
                }
            }
        }
    }

    async fn read_all(&mut self) -> Result<Vec<u8>, Failure> {
        if self.client.is_none() {
            self.client = Some(Client::connect(&self.host, self.port, self.unit, self.wait).await?);
        }
        let Some(client) = self.client.as_mut() else {
            return Err(Failure::Passing("not connected".to_owned()));
        };
        let mut values: Vec<String> = vec!["null".to_owned(); self.points.len()];
        for r in &self.requests {
            let data = client.read(r.table, r.address, r.count).await?;
            for &i in &r.points {
                if let (Some(p), Some(slot)) = (self.points.get(i), values.get_mut(i)) {
                    *slot = render(p, &data, r.address, self.words_little_first);
                }
            }
        }
        let mut out = String::from("{");
        for (i, (p, v)) in self.points.iter().zip(&values).enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&serde_json::Value::from(p.name.as_str()).to_string());
            out.push(':');
            out.push_str(v);
        }
        out.push('}');
        Ok(out.into_bytes())
    }
}

/// One value as JSON, from what its request read.
fn render(p: &Point, data: &Data, start: u16, words_little_first: bool) -> String {
    let offset = usize::from(p.address - start);
    match data {
        Data::Bits(bits) => bits
            .get(offset)
            .map_or("null", |b| if *b { "true" } else { "false" })
            .to_owned(),
        Data::Words(words) => {
            let Some(mine) = words.get(offset..offset + usize::from(p.kind.words())) else {
                return "null".to_owned();
            };
            if let (Some(bit), Some(word)) = (p.bit, mine.first()) {
                return (word >> bit & 1 == 1).to_string();
            }
            let mut ordered = mine.to_vec();
            if words_little_first {
                ordered.reverse();
            }
            let raw = ordered.iter().fold(0u64, |v, w| (v << 16) | u64::from(*w));
            // The casts below keep exactly the bits read: each value is as wide as the registers it came from.
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let number = match p.kind {
                Kind::Bool => return "null".to_owned(),
                Kind::U16 | Kind::U32 | Kind::U64 => Decimal::from(raw),
                Kind::I16 => Decimal::from(raw as u16 as i16),
                Kind::I32 => Decimal::from(raw as u32 as i32),
                Kind::I64 => Decimal::from(raw as i64),
                Kind::F32 => {
                    return float(
                        f64::from(f32::from_bits(raw as u32)),
                        format!("{}", f32::from_bits(raw as u32)),
                        p.scale,
                    );
                }
                Kind::F64 => return float(f64::from_bits(raw), format!("{}", f64::from_bits(raw)), p.scale),
            };
            let scaled = p
                .scale
                .map_or(number, |s| number.checked_mul(s).unwrap_or(number));
            scaled.normalize().to_string()
        }
    }
}

/// A float as JSON: its shortest exact form, scaled exactly where it fits; `null` for NaN and infinity.
fn float(value: f64, shortest: String, scale: Option<Decimal>) -> String {
    if !value.is_finite() {
        return "null".to_owned();
    }
    let Some(scale) = scale else { return shortest };
    match Decimal::from_str(&shortest)
        .ok()
        .and_then(|d| d.checked_mul(scale))
    {
        Some(d) => d.normalize().to_string(),
        None => serde_json::Number::from_f64(value * f64::try_from(scale).unwrap_or(1.0))
            .map_or_else(|| "null".to_owned(), |n| n.to_string()),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn p(table: Table, address: u16, kind: Kind) -> Point {
        Point {
            name: format!("p{address}"),
            table,
            address,
            kind,
            bit: None,
            scale: None,
        }
    }

    #[test]
    fn neighbouring_values_are_read_together_and_gaps_are_not_read() {
        let points = vec![
            p(Table::Holding, 101, Kind::F32),
            p(Table::Holding, 100, Kind::I16),
            p(Table::Holding, 110, Kind::U16),
            p(Table::Coil, 3, Kind::Bool),
            p(Table::Coil, 4, Kind::Bool),
        ];
        let reads: Vec<(Table, u16, u16)> = plan(&points)
            .iter()
            .map(|r| (r.table, r.address, r.count))
            .collect();
        assert_eq!(
            reads,
            vec![
                (Table::Coil, 3, 2),
                (Table::Holding, 100, 3),
                (Table::Holding, 110, 1)
            ]
        );
    }

    #[test]
    fn values_are_read_as_their_type() {
        let words = Data::Words(vec![0xff38, 0x4088, 0x0000, 0x0001, 0xe240]);
        let mut t = p(Table::Holding, 10, Kind::I16);
        assert_eq!(render(&t, &words, 10, false), "-200");
        t.scale = Some(Decimal::from_str("0.1").unwrap());
        assert_eq!(render(&t, &words, 10, false), "-20");
        let f = p(Table::Holding, 11, Kind::F32);
        assert_eq!(render(&f, &words, 10, false), "4.25");
        let u = p(Table::Holding, 13, Kind::U32);
        assert_eq!(render(&u, &words, 10, false), "123456");
        // The same two registers with the low word first.
        assert_eq!(
            render(&u, &Data::Words(vec![0, 0, 0, 0xe240, 0x0001]), 10, true),
            "123456"
        );
        let mut status = p(Table::Holding, 10, Kind::Bool);
        status.bit = Some(3);
        assert_eq!(render(&status, &words, 10, false), "true");
        assert_eq!(
            render(
                &p(Table::Coil, 4, Kind::Bool),
                &Data::Bits(vec![false, true]),
                3,
                false
            ),
            "true"
        );
        // NaN is not a number JSON has.
        assert_eq!(render(&f, &Data::Words(vec![0, 0x7fc0, 0]), 10, false), "null");
    }

    #[test]
    fn a_modbus_url_is_host_and_port() {
        assert_eq!(
            endpoint("modbus://10.0.4.20", "url").unwrap(),
            ("10.0.4.20".to_owned(), 502)
        );
        assert_eq!(endpoint("modbus://plc-7:1502/", "url").unwrap().1, 1502);
        assert!(endpoint("http://plc", "url").is_err());
        assert!(endpoint("modbus://:502", "url").is_err());
    }
}
