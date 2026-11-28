// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! SQL Server's answers, token by token: each result's columns (COLMETADATA) and rows (ROW, NBCROW); how many rows
//! a statement changed (DONE, DONEINPROC); errors; and the changes it reports to the connection (ENVCHANGE): the
//! packet size, its collation, the transaction it is in.

use bytes::{Buf, Bytes};
use serde_json::{Map, Value};

use super::types::{Kind, type_info, value, varchar};
use super::wire::{Connection, take};
use crate::sql::{Answer, Budget};

const COLMETADATA: u8 = 0x81;
const ROW: u8 = 0xd1;
const NBCROW: u8 = 0xd2;
const DONE: u8 = 0xfd;
const DONEPROC: u8 = 0xfe;
const DONEINPROC: u8 = 0xff;
const ERROR: u8 = 0xaa;
const INFO: u8 = 0xab;
const LOGINACK: u8 = 0xad;
const ENVCHANGE: u8 = 0xe3;
const RETURNSTATUS: u8 = 0x79;
const RETURNVALUE: u8 = 0xac;
const ORDER: u8 = 0xa9;
const TABNAME: u8 = 0xa4;
const COLINFO: u8 = 0xa5;
const FEATUREEXTACK: u8 = 0xae;
const SESSIONSTATE: u8 = 0xe4;
const SSPI: u8 = 0xed;
const FEDAUTHINFO: u8 = 0xee;
/// DONE's status: the count is given.
const DONE_COUNT: u16 = 0x10;
const MOST_COLUMNS: u16 = 4096;

/// An error SQL Server sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ServerMessage {
    pub(super) number: u32,
    pub(super) text: String,
}

impl ServerMessage {
    pub(super) fn shown(&self) -> String {
        format!("{} (SQL Server error {})", self.text, self.number)
    }
}

/// What an answer said.
#[derive(Debug, Default)]
pub(super) struct Said {
    pub(super) answers: Vec<Answer>,
    /// The first error.
    pub(super) error: Option<ServerMessage>,
    pub(super) signed_in: bool,
}

fn u16_of(b: &mut Bytes) -> Option<u16> {
    (b.remaining() >= 2).then(|| b.get_u16_le())
}

fn u32_of(b: &mut Bytes) -> Option<u32> {
    (b.remaining() >= 4).then(|| b.get_u32_le())
}

/// A token whose body comes after a two-byte length.
fn sized(b: &mut Bytes) -> Option<Bytes> {
    let n = usize::from(u16_of(b)?);
    take(b, n)
}

fn error_of(mut body: Bytes) -> Option<ServerMessage> {
    let number = u32_of(&mut body)?;
    take(&mut body, 2)?;
    let text = varchar(&mut body, true)?;
    Some(ServerMessage { number, text })
}

/// What ENVCHANGE says, kept on the connection.
fn env_change(mut body: Bytes, c: &mut Connection) -> Option<()> {
    let kind = body.has_remaining().then(|| body.get_u8())?;
    match kind {
        // The packet size, as text.
        4 => {
            let size = varchar(&mut body, false)?;
            c.set_packet(size.trim().parse().unwrap_or(0));
        }
        // The collation, and the transaction begun.
        7 | 8 => {
            let n = usize::from(body.has_remaining().then(|| body.get_u8())?);
            let new = take(&mut body, n)?;
            if kind == 7 && n == 5 {
                c.collation.copy_from_slice(&new);
            }
            if kind == 8 && n == 8 {
                c.transaction.copy_from_slice(&new);
            }
        }
        // Committed, rolled back, or ended.
        9 | 10 | 17 => c.transaction = [0; 8],
        _ => {}
    }
    Some(())
}

/// Reads a whole answer. The rows count against `budget`, when given.
pub(super) fn read(
    message: &Bytes,
    c: &mut Connection,
    mut budget: Option<&mut Budget>,
) -> Result<Said, String> {
    let not_understood = |c: &Connection| format!("{} sent something that is not understood", c.what);
    let mut b = message.clone();
    let mut said = Said::default();
    let mut columns: Vec<(String, Kind)> = Vec::new();
    let mut open: Option<Answer> = None;
    while b.has_remaining() {
        let token = b.get_u8();
        let before = b.remaining();
        let ok = match token {
            COLMETADATA => {
                if let Some(done) = open.take() {
                    said.answers.push(done);
                }
                match metadata(&mut b) {
                    Some(read) => {
                        columns = read;
                        open = Some(Answer {
                            columns: columns.iter().map(|(n, _)| n.clone()).collect(),
                            ..Answer::default()
                        });
                        Some(())
                    }
                    None => None,
                }
            }
            ROW | NBCROW => {
                let nulls = if token == NBCROW {
                    match take(&mut b, columns.len().div_ceil(8)) {
                        Some(n) => Some(n),
                        None => return Err(not_understood(c)),
                    }
                } else {
                    None
                };
                let mut record = Map::new();
                let mut fine = true;
                for (i, (name, kind)) in columns.iter().enumerate() {
                    let null = nulls
                        .as_ref()
                        .is_some_and(|n| n.get(i / 8).is_some_and(|byte| byte & (1 << (i % 8)) != 0));
                    let v = if null {
                        Some(Value::Null)
                    } else {
                        value(kind, &mut b)
                    };
                    match v {
                        Some(v) => {
                            record.insert(name.clone(), v);
                        }
                        None => {
                            fine = false;
                            break;
                        }
                    }
                }
                if !fine {
                    return Err(not_understood(c));
                }
                let answer = open.get_or_insert_with(Answer::default);
                if let Some(budget) = budget.as_deref_mut() {
                    budget.row(answer.rows.len() + 1, before - b.remaining())?;
                }
                answer.rows.push(record);
                Some(())
            }
            DONE | DONEPROC | DONEINPROC => match (u16_of(&mut b), u16_of(&mut b), take(&mut b, 8)) {
                (Some(status), Some(_), Some(mut count)) => {
                    match open.take() {
                        Some(done) => said.answers.push(done),
                        None if token != DONEPROC && status & DONE_COUNT != 0 => said.answers.push(Answer {
                            affected: Some(count.get_u64_le()),
                            ..Answer::default()
                        }),
                        None => {}
                    }
                    Some(())
                }
                _ => None,
            },
            ERROR => sized(&mut b).map(|body| {
                if said.error.is_none() {
                    said.error = error_of(body);
                }
            }),
            LOGINACK => sized(&mut b).map(|_| said.signed_in = true),
            ENVCHANGE => sized(&mut b).and_then(|body| env_change(body, c)),
            INFO | ORDER | TABNAME | COLINFO | SSPI => sized(&mut b).map(|_| ()),
            RETURNSTATUS => take(&mut b, 4).map(|_| ()),
            RETURNVALUE => return_value(&mut b),
            FEATUREEXTACK => loop {
                match b.has_remaining().then(|| b.get_u8()) {
                    Some(0xff) => break Some(()),
                    Some(_) => {
                        let Some(n) = u32_of(&mut b).and_then(|n| usize::try_from(n).ok()) else {
                            break None;
                        };
                        if take(&mut b, n).is_none() {
                            break None;
                        }
                    }
                    None => break None,
                }
            },
            SESSIONSTATE | FEDAUTHINFO => u32_of(&mut b)
                .and_then(|n| usize::try_from(n).ok())
                .and_then(|n| take(&mut b, n))
                .map(|_| ()),
            _ => None,
        };
        if ok.is_none() {
            return Err(not_understood(c));
        }
    }
    if let Some(done) = open.take() {
        said.answers.push(done);
    }
    Ok(said)
}

/// COLMETADATA's columns: each one's name and type.
fn metadata(b: &mut Bytes) -> Option<Vec<(String, Kind)>> {
    let count = u16_of(b)?;
    if count == 0xffff {
        return Some(Vec::new());
    }
    if count > MOST_COLUMNS {
        return None;
    }
    let mut out = Vec::with_capacity(usize::from(count));
    for i in 0..count {
        // The user type, and the column's flags.
        take(b, 6)?;
        let kind = type_info(b)?;
        let mut name = varchar(b, false)?;
        if name.is_empty() {
            name = format!("column_{}", i + 1);
        }
        out.push((name, kind));
    }
    Some(out)
}

/// A procedure's output parameter: read past.
fn return_value(b: &mut Bytes) -> Option<()> {
    take(b, 2)?;
    varchar(b, false)?;
    take(b, 1 + 4 + 2)?;
    let kind = type_info(b)?;
    value(&kind, b).map(|_| ())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
pub(super) mod tests {
    use bytes::BufMut;

    use super::super::wire::ucs2;
    use super::*;

    /// A B_VARCHAR.
    pub(in super::super) fn b_varchar(out: &mut Vec<u8>, text: &str) {
        out.push(u8::try_from(text.encode_utf16().count()).unwrap());
        out.extend_from_slice(&ucs2(text));
    }

    /// COLMETADATA for (name, TYPE_INFO) columns.
    pub(in super::super) fn columns(out: &mut Vec<u8>, of: &[(&str, &[u8])]) {
        out.push(COLMETADATA);
        out.put_u16_le(u16::try_from(of.len()).unwrap());
        for (name, info) in of {
            out.extend_from_slice(&[0, 0, 0, 0, 0x09, 0x00]);
            out.extend_from_slice(info);
            b_varchar(out, name);
        }
    }

    pub(in super::super) fn done(out: &mut Vec<u8>, token: u8, count: Option<u64>) {
        out.push(token);
        out.put_u16_le(if count.is_some() { DONE_COUNT } else { 0 });
        out.put_u16_le(0xc1);
        out.put_u64_le(count.unwrap_or(0));
    }

    pub(in super::super) fn error(out: &mut Vec<u8>, number: u32, text: &str) {
        let mut body = Vec::new();
        body.put_u32_le(number);
        body.extend_from_slice(&[1, 16]);
        body.put_u16_le(u16::try_from(text.encode_utf16().count()).unwrap());
        body.extend_from_slice(&ucs2(text));
        b_varchar(&mut body, "db");
        b_varchar(&mut body, "");
        body.put_u32_le(1);
        out.push(ERROR);
        out.put_u16_le(u16::try_from(body.len()).unwrap());
        out.extend_from_slice(&body);
    }

    pub(in super::super) fn env(out: &mut Vec<u8>, kind: u8, new: &[u8], old: &[u8]) {
        out.push(ENVCHANGE);
        out.put_u16_le(u16::try_from(3 + new.len() + old.len()).unwrap());
        out.push(kind);
        out.push(u8::try_from(new.len()).unwrap());
        out.extend_from_slice(new);
        out.push(u8::try_from(old.len()).unwrap());
        out.extend_from_slice(old);
    }

    fn connection() -> Connection {
        Connection::for_tests()
    }

    #[test]
    fn results_rows_counts_and_errors_are_read() {
        let mut m = Vec::new();
        env(&mut m, 8, &[1, 2, 3, 4, 5, 6, 7, 8], &[]);
        columns(
            &mut m,
            &[
                ("id", &[0x26, 4]),
                ("name", &[0xe7, 0x64, 0, 9, 4, 0xd0, 0, 0x34]),
            ],
        );
        m.push(ROW);
        m.extend_from_slice(&[4, 7, 0, 0, 0]);
        m.extend_from_slice(&[4, 0, b'A', 0, b'b', 0]);
        // A row with its name null, by the bitmap.
        m.push(NBCROW);
        m.push(0b10);
        m.extend_from_slice(&[4, 8, 0, 0, 0]);
        done(&mut m, DONEINPROC, Some(2));
        // An UPDATE: no columns, a count.
        done(&mut m, DONEINPROC, Some(5));
        m.extend_from_slice(&[RETURNSTATUS, 0, 0, 0, 0]);
        done(&mut m, DONEPROC, None);
        let mut c = connection();
        let said = read(&Bytes::from(m), &mut c, None).unwrap();
        assert_eq!(c.transaction, [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(said.answers.len(), 2);
        assert_eq!(said.answers[0].columns, vec!["id", "name"]);
        assert_eq!(said.answers[0].rows[0]["id"], 7);
        assert_eq!(said.answers[0].rows[0]["name"], "Ab");
        assert_eq!(said.answers[0].rows[1]["name"], Value::Null);
        assert_eq!(said.answers[1].affected, Some(5));
        assert!(said.error.is_none());

        let mut m = Vec::new();
        error(&mut m, 208, "Invalid object name 'nope'.");
        done(&mut m, DONE, None);
        env(&mut m, 10, &[], &[1, 2, 3, 4, 5, 6, 7, 8]);
        let said = read(&Bytes::from(m), &mut c, None).unwrap();
        assert_eq!(
            said.error.unwrap().shown(),
            "Invalid object name 'nope'. (SQL Server error 208)"
        );
        assert_eq!(c.transaction, [0; 8]);
    }

    #[test]
    fn what_is_cut_short_or_unknown_is_refused() {
        let mut m = Vec::new();
        columns(&mut m, &[("id", &[0x26, 4])]);
        m.push(ROW);
        m.extend_from_slice(&[4, 7, 0]);
        let mut c = connection();
        assert!(read(&Bytes::from(m.clone()), &mut c, None).is_err());
        for cut in 1..m.len() - 4 {
            assert!(
                read(&Bytes::copy_from_slice(&m[..cut]), &mut c, None).is_err(),
                "cut at {cut}"
            );
        }
        assert!(read(&Bytes::from_static(&[0x42, 0, 0]), &mut c, None).is_err());
    }

    #[test]
    fn rows_past_the_limits_are_refused() {
        let mut m = Vec::new();
        columns(&mut m, &[("n", &[0x26, 1])]);
        for i in 0..5u8 {
            m.extend_from_slice(&[ROW, 1, i]);
        }
        done(&mut m, DONE, Some(5));
        let mut budget = Budget::new(crate::sql::Limits {
            max_rows: 3,
            max_bytes: 1 << 20,
            timeout: std::time::Duration::from_secs(1),
        });
        let mut c = connection();
        let e = read(&Bytes::from(m), &mut c, Some(&mut budget)).unwrap_err();
        assert!(e.contains("more than 3 rows"), "{e}");
    }
}
