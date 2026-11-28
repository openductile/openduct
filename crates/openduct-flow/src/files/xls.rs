// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! XLS spreadsheets (the binary format of Excel 97 to 2003): the workbook stream read out of its compound file, then
//! a sheet's cells from its records - shared texts, numbers, formulas' last values, true or false - dates as the
//! cell's format shows them. Every chain and length is checked, so a damaged or hostile file is refused, not
//! followed.

use std::collections::BTreeMap;

use super::csv::{MAX_CELLS, MAX_COLUMNS, MAX_ROWS};
use super::extract::{Cell, Grid, builtin_date, date_format, serial_date};

const BROKEN: &str = "not an XLS spreadsheet, or a damaged one";
const END_OF_CHAIN: u32 = 0xffff_fffe;
const FREE: u32 = 0xffff_ffff;

fn u16_at(b: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(b.get(at..at + 2)?.try_into().ok()?))
}

fn u32_at(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

fn index(n: u32) -> usize {
    usize::try_from(n).unwrap_or(usize::MAX)
}

/// A compound file: its sectors, their chains, and its directory.
struct Compound<'a> {
    bytes: &'a [u8],
    sector: usize,
    fat: Vec<u32>,
}

impl<'a> Compound<'a> {
    fn open(bytes: &'a [u8]) -> Result<Self, String> {
        if bytes.get(..8) != Some(&[0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1]) {
            return Err(BROKEN.to_owned());
        }
        let shift = u16_at(bytes, 0x1e).ok_or(BROKEN)?;
        if shift != 9 && shift != 12 {
            return Err(BROKEN.to_owned());
        }
        let mut file = Self {
            bytes,
            sector: 1 << shift,
            fat: Vec::new(),
        };
        // Where the allocation table's sectors are: 109 in the header, the rest in a chain of their own.
        let mut fat_sectors: Vec<u32> = (0..109)
            .filter_map(|i| u32_at(bytes, 0x4c + i * 4))
            .filter(|s| *s != FREE && *s != END_OF_CHAIN)
            .collect();
        let mut next = u32_at(bytes, 0x44).ok_or(BROKEN)?;
        let per = file.sector / 4;
        let mut guard = 0;
        while next != END_OF_CHAIN && next != FREE {
            guard += 1;
            if guard > bytes.len() / file.sector {
                return Err(BROKEN.to_owned());
            }
            let sector = file.sector_bytes(next)?;
            fat_sectors.extend(
                (0..per - 1)
                    .filter_map(|i| u32_at(sector, i * 4))
                    .filter(|s| *s != FREE && *s != END_OF_CHAIN),
            );
            next = u32_at(sector, (per - 1) * 4).ok_or(BROKEN)?;
        }
        if fat_sectors.len() > bytes.len() / file.sector + 1 {
            return Err(BROKEN.to_owned());
        }
        let mut fat = Vec::new();
        for s in fat_sectors {
            let sector = file.sector_bytes(s)?;
            fat.extend((0..per).filter_map(|i| u32_at(sector, i * 4)));
        }
        file.fat = fat;
        Ok(file)
    }

    fn sector_bytes(&self, n: u32) -> Result<&'a [u8], String> {
        let start = index(n)
            .checked_add(1)
            .and_then(|i| i.checked_mul(self.sector))
            .ok_or(BROKEN)?;
        self.bytes
            .get(start..start + self.sector)
            .ok_or_else(|| BROKEN.to_owned())
    }

    /// The bytes of the chain starting at `start`, through `table`, each unit read by `unit_at`: `size` of them, or
    /// (`None`) all the chain holds.
    fn chain(
        table: &[u32],
        start: u32,
        size: Option<usize>,
        mut unit_at: impl FnMut(u32) -> Result<Vec<u8>, String>,
    ) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        let mut at = start;
        let mut steps = 0;
        while at != END_OF_CHAIN && size.is_none_or(|s| out.len() < s) {
            steps += 1;
            if steps > table.len() + 1 {
                return Err(BROKEN.to_owned());
            }
            out.extend_from_slice(&unit_at(at)?);
            at = *table.get(index(at)).ok_or(BROKEN)?;
        }
        if let Some(size) = size {
            if out.len() < size {
                return Err(BROKEN.to_owned());
            }
            out.truncate(size);
        }
        Ok(out)
    }

    fn stream(&self, start: u32, size: usize) -> Result<Vec<u8>, String> {
        Self::chain(&self.fat, start, Some(size), |s| {
            self.sector_bytes(s).map(<[u8]>::to_vec)
        })
    }

    /// The stream called one of `names`: from the sectors, or (a small one) from the mini stream.
    fn named(&self, names: &[&str]) -> Result<Option<Vec<u8>>, String> {
        let first = u32_at(self.bytes, 0x30).ok_or(BROKEN)?;
        let directory = Self::chain(&self.fat, first, None, |s| {
            self.sector_bytes(s).map(<[u8]>::to_vec)
        })?;
        let entry = |i: usize| directory.get(i * 128..i * 128 + 128);
        let root = entry(0).ok_or(BROKEN)?;
        let cutoff = index(u32_at(self.bytes, 0x38).ok_or(BROKEN)?);
        for i in 1..directory.len() / 128 {
            let Some(e) = entry(i) else { break };
            let len = usize::from(u16_at(e, 64).ok_or(BROKEN)?).min(64);
            let name: Vec<u16> = (0..len.saturating_sub(2) / 2)
                .filter_map(|c| u16_at(e, c * 2))
                .collect();
            let name = String::from_utf16_lossy(&name);
            if e.get(66) != Some(&2) || !names.iter().any(|n| n.eq_ignore_ascii_case(&name)) {
                continue;
            }
            let start = u32_at(e, 116).ok_or(BROKEN)?;
            let size = index(u32_at(e, 120).ok_or(BROKEN)?);
            if size > self.bytes.len() {
                return Err(BROKEN.to_owned());
            }
            if size >= cutoff {
                return self.stream(start, size).map(Some);
            }
            let mini_size = index(u32_at(root, 120).ok_or(BROKEN)?).min(self.bytes.len());
            let mini = self.stream(u32_at(root, 116).ok_or(BROKEN)?, mini_size)?;
            let mini_fat_start = u32_at(self.bytes, 0x3c).ok_or(BROKEN)?;
            let mini_fat_sectors = index(u32_at(self.bytes, 0x40).ok_or(BROKEN)?);
            let table_bytes = self.stream(
                mini_fat_start,
                mini_fat_sectors.saturating_mul(self.sector).min(self.bytes.len()),
            )?;
            let table: Vec<u32> = (0..table_bytes.len() / 4)
                .filter_map(|i| u32_at(&table_bytes, i * 4))
                .collect();
            let unit = 64usize;
            return Self::chain(&table, start, Some(size), |s| {
                let at = index(s).saturating_mul(unit);
                mini.get(at..at + unit)
                    .map(<[u8]>::to_vec)
                    .ok_or_else(|| BROKEN.to_owned())
            })
            .map(Some);
        }
        Ok(None)
    }
}

/// Bytes read across a record and the CONTINUE records after it: a text's characters split between two records
/// start again, in the second, with their width.
struct Continued<'a> {
    parts: Vec<&'a [u8]>,
    part: usize,
    at: usize,
}

impl<'a> Continued<'a> {
    fn new(parts: Vec<&'a [u8]>) -> Self {
        Self {
            parts,
            part: 0,
            at: 0,
        }
    }

    fn left_here(&self) -> usize {
        self.parts
            .get(self.part)
            .map_or(0, |p| p.len().saturating_sub(self.at))
    }

    fn next_part(&mut self) -> Option<()> {
        self.part += 1;
        self.at = 0;
        self.parts.get(self.part).map(|_| ())
    }

    fn byte(&mut self) -> Option<u8> {
        while self.left_here() == 0 {
            self.next_part()?;
        }
        let b = *self.parts.get(self.part)?.get(self.at)?;
        self.at += 1;
        Some(b)
    }

    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes([self.byte()?, self.byte()?]))
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes([
            self.byte()?,
            self.byte()?,
            self.byte()?,
            self.byte()?,
        ]))
    }

    fn skip(&mut self, mut n: usize) -> Option<()> {
        while n > 0 {
            while self.left_here() == 0 {
                self.next_part()?;
            }
            let step = n.min(self.left_here());
            self.at += step;
            n -= step;
        }
        Some(())
    }

    fn chars(&mut self, count: usize, mut wide: bool) -> Option<String> {
        let mut units: Vec<u16> = Vec::with_capacity(count.min(32_768));
        while units.len() < count {
            if self.left_here() == 0 {
                self.next_part()?;
                wide = self.byte()? & 1 == 1;
                continue;
            }
            units.push(if wide {
                self.u16()?
            } else {
                u16::from(self.byte()?)
            });
        }
        Some(String::from_utf16_lossy(&units))
    }

    /// An `XLUnicodeRichExtendedString`, as the shared texts hold them.
    fn rich_text(&mut self) -> Option<String> {
        let count = usize::from(self.u16()?);
        let flags = self.byte()?;
        let runs = if flags & 8 != 0 {
            usize::from(self.u16()?)
        } else {
            0
        };
        let extra = if flags & 4 != 0 { index(self.u32()?) } else { 0 };
        let text = self.chars(count, flags & 1 == 1)?;
        self.skip(runs * 4)?;
        self.skip(extra)?;
        Some(text)
    }
}

/// The records of a stream from `at`: each its type and its data, up to the end of the substream.
fn records(stream: &[u8], mut at: usize) -> Vec<(u16, &[u8])> {
    let mut out = Vec::new();
    while let (Some(kind), Some(len)) = (u16_at(stream, at), u16_at(stream, at + 2)) {
        let Some(data) = stream.get(at + 4..at + 4 + usize::from(len)) else {
            break;
        };
        out.push((kind, data));
        at += 4 + usize::from(len);
        if kind == 0x000a {
            break;
        }
    }
    out
}

/// A record and the CONTINUE records after it.
fn with_continues<'a>(list: &[(u16, &'a [u8])], i: usize) -> Vec<&'a [u8]> {
    let mut parts: Vec<&[u8]> = list.get(i).map(|(_, d)| *d).into_iter().collect();
    parts.extend(
        list.iter()
            .skip(i + 1)
            .take_while(|(k, _)| *k == 0x003c)
            .map(|(_, d)| *d),
    );
    parts
}

/// A number stored in four bytes (RK): an integer or the top of a float, perhaps a hundredth of it.
fn rk(value: u32) -> f64 {
    let n = if value & 2 != 0 {
        #[allow(clippy::cast_possible_wrap)] // the bits are a signed integer
        f64::from((value as i32) >> 2)
    } else {
        f64::from_bits(u64::from(value & 0xffff_fffc) << 32)
    };
    if value & 1 != 0 { n / 100.0 } else { n }
}

fn number_text(n: f64) -> String {
    if n.fract() == 0.0 && n.abs() < 1e15 {
        format!("{n:.0}")
    } else {
        format!("{n}")
    }
}

/// What the workbook's globals say: its sheets (name and where), shared texts, each cell format's number format,
/// the formats' codes, and whether dates count from 1904.
#[derive(Default)]
struct Globals {
    sheets: Vec<(String, usize)>,
    strings: Vec<String>,
    formats: Vec<u16>,
    codes: Vec<(u16, String)>,
    from_1904: bool,
}

fn globals(stream: &[u8]) -> Result<Globals, String> {
    let list = records(stream, 0);
    match list.first() {
        Some((0x0809, data)) if u16_at(data, 0) == Some(0x0600) => {}
        Some((0x0809 | 0x0409 | 0x0209 | 0x0009, _)) => {
            return Err("an XLS file older than Excel 97: save it again as XLSX".to_owned());
        }
        _ => return Err(BROKEN.to_owned()),
    }
    let mut g = Globals::default();
    for (i, (kind, data)) in list.iter().enumerate() {
        match kind {
            0x0085 => {
                let at = index(u32_at(data, 0).ok_or(BROKEN)?);
                let mut name = Continued::new(vec![data.get(6..).unwrap_or_default()]);
                let count = usize::from(data.get(6).copied().unwrap_or(0));
                let wide = data.get(7).is_some_and(|f| f & 1 == 1);
                name.skip(2);
                let name = name.chars(count, wide).ok_or(BROKEN)?;
                // Worksheets only: not charts, macros or modules.
                if data.get(5) == Some(&0) {
                    g.sheets.push((name, at));
                }
            }
            0x00fc => {
                let mut texts = Continued::new(with_continues(&list, i));
                texts.skip(4);
                let unique = index(texts.u32().ok_or(BROKEN)?);
                for _ in 0..unique.min(MAX_ROWS * 10) {
                    match texts.rich_text() {
                        Some(t) => g.strings.push(t),
                        None => break,
                    }
                }
            }
            0x00e0 => g.formats.push(u16_at(data, 2).unwrap_or(0)),
            0x041e => {
                let id = u16_at(data, 0).unwrap_or(0);
                let count = usize::from(u16_at(data, 2).unwrap_or(0));
                let wide = data.get(4).is_some_and(|f| f & 1 == 1);
                let mut code = Continued::new(vec![data.get(5..).unwrap_or_default()]);
                g.codes.push((id, code.chars(count, wide).unwrap_or_default()));
            }
            0x0022 => g.from_1904 = u16_at(data, 0) == Some(1),
            _ => {}
        }
    }
    Ok(g)
}

/// The sheet called `wanted` (the first, when not given) of an XLS workbook, as rows of cells.
pub(super) fn read(bytes: &[u8], wanted: Option<&str>) -> Result<Grid, String> {
    let file = Compound::open(bytes)?;
    let stream = match file.named(&["Workbook"])? {
        Some(s) => s,
        None if file.named(&["Book"])?.is_some() => {
            return Err("an XLS file older than Excel 97: save it again as XLSX".to_owned());
        }
        None => return Err(BROKEN.to_owned()),
    };
    let g = globals(&stream)?;
    let names: Vec<String> = g.sheets.iter().map(|(n, _)| n.clone()).collect();
    let (name, at) = match wanted {
        Some(w) => g
            .sheets
            .iter()
            .find(|(n, _)| n == w)
            .ok_or_else(|| format!("there is no sheet {w:?}: it has {}", names.join(", ")))?,
        None => g.sheets.first().ok_or("the workbook has no sheets")?,
    };
    let is_date = |xf: u16| {
        let format = g.formats.get(usize::from(xf)).copied().unwrap_or(0);
        g.codes
            .iter()
            .find(|(id, _)| *id == format)
            .map_or_else(|| builtin_date(u32::from(format)), |(_, code)| date_format(code))
    };
    let number = |xf: u16, n: f64| {
        if is_date(xf)
            && let Some(date) = serial_date(n, g.from_1904)
        {
            return Cell::Date(date);
        }
        Cell::Number(number_text(n))
    };
    let mut grid: BTreeMap<u16, Vec<Cell>> = BTreeMap::new();
    let mut held = 0usize;
    let mut put = |row: u16, column: u16, cell: Cell| -> Result<(), String> {
        let column = usize::from(column);
        if column >= MAX_COLUMNS {
            return Err(format!("a row of more than {MAX_COLUMNS} columns"));
        }
        if usize::from(row) > MAX_ROWS {
            return Err(format!("more than {MAX_ROWS} rows"));
        }
        let cells = grid.entry(row).or_default();
        if cells.len() <= column {
            held += column + 1 - cells.len();
            if held > MAX_CELLS {
                return Err(format!("a sheet of more than {MAX_CELLS} cells"));
            }
            cells.resize(column + 1, Cell::Empty);
        }
        if let Some(place) = cells.get_mut(column) {
            *place = cell;
        }
        Ok(())
    };
    let list = records(&stream, *at);
    let mut formula_at: Option<(u16, u16)> = None;
    for (i, (kind, data)) in list.iter().enumerate() {
        let (row, column, xf) = (
            u16_at(data, 0).unwrap_or(0),
            u16_at(data, 2).unwrap_or(0),
            u16_at(data, 4).unwrap_or(0),
        );
        match kind {
            0x00fd => {
                let text = u32_at(data, 6)
                    .and_then(|s| g.strings.get(index(s)))
                    .cloned()
                    .unwrap_or_default();
                put(row, column, Cell::Text(text))?;
            }
            0x0203 => {
                let n = data
                    .get(6..14)
                    .and_then(|b| b.try_into().ok())
                    .map_or(0.0, f64::from_le_bytes);
                put(row, column, number(xf, n))?;
            }
            0x027e => put(row, column, number(xf, rk(u32_at(data, 6).unwrap_or(0))))?,
            0x00bd => {
                let count = data.len().saturating_sub(6) / 6;
                for k in 0..count {
                    let xf = u16_at(data, 4 + k * 6).unwrap_or(0);
                    let value = u32_at(data, 6 + k * 6).unwrap_or(0);
                    let column = column.saturating_add(u16::try_from(k).unwrap_or(u16::MAX));
                    put(row, column, number(xf, rk(value)))?;
                }
            }
            0x0006 => {
                let result = data.get(6..14).unwrap_or_default();
                formula_at = None;
                if result.get(6..8) == Some(&[0xff, 0xff]) {
                    match result.first() {
                        Some(0) => formula_at = Some((row, column)),
                        Some(1) => put(row, column, Cell::Bool(result.get(2) == Some(&1)))?,
                        _ => {}
                    }
                } else if let Ok(b) = <[u8; 8]>::try_from(result) {
                    put(row, column, number(xf, f64::from_le_bytes(b)))?;
                }
            }
            0x0207 => {
                if let Some((row, column)) = formula_at.take() {
                    let mut text = Continued::new(with_continues(&list, i));
                    let count = usize::from(text.u16().unwrap_or(0));
                    let wide = text.byte().is_some_and(|f| f & 1 == 1);
                    put(
                        row,
                        column,
                        Cell::Text(text.chars(count, wide).unwrap_or_default()),
                    )?;
                }
            }
            0x0204 | 0x00d6 => {
                let count = usize::from(u16_at(data, 6).unwrap_or(0));
                let wide = data.get(8).is_some_and(|f| f & 1 == 1);
                let mut text = Continued::new(vec![data.get(9..).unwrap_or_default()]);
                put(
                    row,
                    column,
                    Cell::Text(text.chars(count, wide).unwrap_or_default()),
                )?;
            }
            0x0205 if data.get(7) == Some(&0) => put(row, column, Cell::Bool(data.get(6) == Some(&1)))?,
            _ => {}
        }
    }
    Ok(Grid {
        sheet: Some(name.clone()),
        sheets: names,
        rows: grid.into_values().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_in_four_bytes_read_as_written() {
        assert_eq!(rk((12 << 2) | 2), 12.0);
        assert_eq!(rk((1234 << 2) | 3), 12.34);
        assert_eq!(rk(0x3ff0_0000), 1.0);
        assert_eq!(number_text(3.0), "3");
        assert_eq!(number_text(0.1), "0.1");
    }

    #[test]
    fn a_text_split_between_records_reads_whole() {
        // "abcd": two narrow characters, then the rest in a CONTINUE record that starts with its width (wide).
        let first = [4u8, 0, 0, b'a', b'b'];
        let second = [1u8, b'c', 0, b'd', 0];
        let mut texts = Continued::new(vec![&first, &second]);
        assert_eq!(texts.rich_text().as_deref(), Some("abcd"));
        assert!(Continued::new(vec![&[9, 0, 0, b'x']]).rich_text().is_none());
    }

    #[test]
    fn what_is_not_a_compound_file_is_refused() {
        assert!(read(b"not a spreadsheet", None).is_err());
        let mut header = vec![0u8; 512];
        header[..8].copy_from_slice(&[0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1]);
        header[0x1e] = 9;
        assert!(read(&header, None).is_err());
    }
}
