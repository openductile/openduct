// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The paths a storage step names, checked before anything is reached. A path is plain names separated by `/`:
//! no `..` (it would lead out of the folder), no backslash, no NUL or other control character, each name at most
//! [`MAX_NAME`] bytes, at most [`MAX_DEPTH`] deep and [`MAX_PATH`] long. A path from the top (`/upload/in`) is taken
//! only where the place allows it: a server without a folder of its own in its credential. A folder on this machine
//! never does - its paths are inside the folder the installation's admin allowed.

/// The longest path, in bytes.
pub(crate) const MAX_PATH: usize = 1024;
/// The longest name within a path, in bytes.
const MAX_NAME: usize = 255;
/// The most names in a path.
pub(crate) const MAX_DEPTH: usize = 32;
/// The longest name pattern.
const MAX_PATTERN: usize = 200;

/// A path, checked: its names in order, and whether it is from the top of the place.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Rel {
    pub(crate) parts: Vec<String>,
    pub(crate) absolute: bool,
}

impl Rel {
    /// `text` as a path; one from the top (`/...`) only when `absolute_allowed`.
    pub(crate) fn parse(text: &str, absolute_allowed: bool) -> Result<Self, String> {
        let shown: String = text.chars().take(80).collect();
        if text.len() > MAX_PATH {
            return Err(format!("{shown:?}...: a path of at most {MAX_PATH} bytes"));
        }
        if text.chars().any(|c| c.is_control()) {
            return Err(format!(
                "{shown:?}: a path holds no control characters (a line break, a NUL)"
            ));
        }
        let text = text.trim();
        if text.contains('\\') {
            return Err(format!("{shown:?}: names are separated by /, not \\"));
        }
        let bytes = text.as_bytes();
        if bytes.get(1) == Some(&b':') && bytes.first().is_some_and(u8::is_ascii_alphabetic) {
            return Err(format!("{shown:?}: a path inside the folder, not a drive's"));
        }
        let absolute = text.starts_with('/');
        if absolute && !absolute_allowed {
            return Err(format!(
                "{shown:?}: a path inside the folder (as in/orders.csv), not one from the top of the disk"
            ));
        }
        let mut parts = Vec::new();
        for part in text.split('/') {
            match part {
                "" | "." => {}
                ".." => {
                    return Err(format!(
                        "{shown:?}: .. would lead out of the folder, and is refused"
                    ));
                }
                name if name.len() > MAX_NAME => {
                    return Err(format!("{shown:?}: a name of at most {MAX_NAME} bytes"));
                }
                name => parts.push(name.to_owned()),
            }
        }
        if parts.len() > MAX_DEPTH {
            return Err(format!("{shown:?}: at most {MAX_DEPTH} folders deep"));
        }
        Ok(Self { parts, absolute })
    }

    /// The place's own top: no names.
    pub(crate) fn is_top(&self) -> bool {
        self.parts.is_empty()
    }

    /// The last name: a file's, or a folder's.
    pub(crate) fn name(&self) -> Option<&str> {
        self.parts.last().map(String::as_str)
    }

    /// The folder it is in.
    pub(crate) fn parent(&self) -> Self {
        let mut parts = self.parts.clone();
        parts.pop();
        Self {
            parts,
            absolute: self.absolute,
        }
    }

    /// `name` inside this folder.
    pub(crate) fn child(&self, name: &str) -> Self {
        let mut parts = self.parts.clone();
        parts.push(name.to_owned());
        Self {
            parts,
            absolute: self.absolute,
        }
    }

    /// The path as text: `in/orders.csv`, or `/upload/in` from the top.
    pub(crate) fn text(&self) -> String {
        let joined = self.parts.join("/");
        if self.absolute {
            format!("/{joined}")
        } else {
            joined
        }
    }

    /// The path inside `base` (a credential's own folder, itself a path from the top or not).
    pub(crate) fn under(&self, base: &Self) -> Self {
        let mut parts = base.parts.clone();
        parts.extend(self.parts.iter().cloned());
        Self {
            parts,
            absolute: base.absolute,
        }
    }

    /// The path as seen from `base`, when it is inside it.
    pub(crate) fn inside(&self, base: &Self) -> Option<Self> {
        let rest = self.parts.strip_prefix(base.parts.as_slice())?;
        Some(Self {
            parts: rest.to_vec(),
            absolute: false,
        })
    }
}

/// A name pattern: `*` for any run of characters, `?` for one, letters' case not counted; several separated by
/// commas (`*.csv, *.xlsx`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pattern {
    each: Vec<Vec<char>>,
}

impl Pattern {
    pub fn parse(text: &str) -> Result<Self, String> {
        if text.len() > MAX_PATTERN {
            return Err(format!("a name pattern of at most {MAX_PATTERN} characters"));
        }
        let each: Vec<Vec<char>> = text
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(|p| p.to_lowercase().chars().collect())
            .collect();
        if each.is_empty() {
            return Err("a name pattern, as *.csv".to_owned());
        }
        Ok(Self { each })
    }

    pub fn matches(&self, name: &str) -> bool {
        let name: Vec<char> = name.to_lowercase().chars().collect();
        self.each.iter().any(|p| glob(p, &name))
    }
}

/// Whether `name` matches `pattern`, in time bounded by their lengths' product: a star remembers where it was.
fn glob(pattern: &[char], name: &[char]) -> bool {
    let (mut p, mut n) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while n < name.len() {
        match pattern.get(p) {
            Some('*') => {
                star = Some((p, n));
                p += 1;
            }
            Some(c) if *c == '?' || Some(c) == name.get(n) => {
                p += 1;
                n += 1;
            }
            _ => match star {
                Some((sp, sn)) => {
                    p = sp + 1;
                    n = sn + 1;
                    star = Some((sp, sn + 1));
                }
                None => return false,
            },
        }
    }
    pattern
        .get(p..)
        .is_some_and(|rest| rest.iter().all(|c| *c == '*'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok(text: &str) -> Rel {
        Rel::parse(text, false).unwrap_or_else(|e| unreachable!("{text}: {e}"))
    }

    fn refused(text: &str, absolute_allowed: bool) -> String {
        Rel::parse(text, absolute_allowed).err().unwrap_or_default()
    }

    #[test]
    fn a_path_is_plain_names_inside_the_folder() {
        assert_eq!(ok("in/orders.csv").parts, ["in", "orders.csv"]);
        assert_eq!(ok("./in//a b.csv/").parts, ["in", "a b.csv"]);
        assert_eq!(ok("in/./x").text(), "in/x");
        assert!(ok("").is_top() && ok(".").is_top() && ok("  ").is_top());
        assert_eq!(ok("in/orders.csv").name(), Some("orders.csv"));
        assert_eq!(ok("in/orders.csv").parent().text(), "in");
        assert_eq!(ok("in").child("x.csv").text(), "in/x.csv");
        assert_eq!(ok("...hidden").parts, ["...hidden"], "three dots are a name");
        assert_eq!(ok("rapport été.csv").parts, ["rapport été.csv"]);
    }

    #[test]
    fn a_path_that_would_lead_out_is_refused() {
        for bad in ["../etc/passwd", "in/../../etc", "in/..", "..", "a/b/../../.."] {
            assert!(refused(bad, false).contains(".. would lead out"), "{bad}");
            assert!(
                refused(bad, true).contains(".. would lead out"),
                "{bad} (from the top too)"
            );
        }
        for bad in ["/etc/passwd", "//server/share", " /x"] {
            assert!(refused(bad, false).contains("not one from the top"), "{bad}");
        }
        assert!(refused("C:/Windows", false).contains("drive"));
        assert!(refused("c:x", false).contains("drive"));
        assert!(refused("in\\..\\x", false).contains("not \\"));
        assert!(refused("a\0b", false).contains("control"));
        assert!(refused("a\nb", false).contains("control"));
        assert!(refused(&"x".repeat(MAX_PATH + 1), false).contains("at most"));
        assert!(refused(&format!("a/{}", "n".repeat(256)), false).contains("a name of at most"));
        assert!(refused(&"a/".repeat(MAX_DEPTH + 1), false).contains("deep"));
    }

    #[test]
    fn a_path_from_the_top_is_taken_where_the_place_allows_it() {
        let top = Rel::parse("/upload/in", true).unwrap_or_default();
        assert!(top.absolute);
        assert_eq!(top.text(), "/upload/in");
        let base = Rel::parse("/home/acme", true).unwrap_or_default();
        let inner = ok("in/a.csv").under(&base);
        assert_eq!(inner.text(), "/home/acme/in/a.csv");
        assert_eq!(inner.inside(&base).map(|r| r.text()), Some("in/a.csv".to_owned()));
        assert_eq!(ok("x").under(&base).inside(&ok("other")), None);
    }

    #[test]
    fn names_are_matched_by_a_pattern() {
        let p = Pattern::parse("*.csv, report-??.xlsx").unwrap_or_else(|e| unreachable!("{e}"));
        assert!(p.matches("orders.csv") && p.matches("ORDERS.CSV") && p.matches(".csv"));
        assert!(p.matches("report-07.xlsx") && !p.matches("report-7.xlsx"));
        assert!(!p.matches("orders.csv.bak"));
        let any = Pattern::parse("*").unwrap_or_else(|e| unreachable!("{e}"));
        assert!(any.matches("") && any.matches("anything"));
        assert!(Pattern::parse(" , ").is_err());
        assert!(Pattern::parse(&"*".repeat(201)).is_err());
        // A pattern made to backtrack takes time bounded by the lengths, not exponential.
        let slow = Pattern::parse(&"*a".repeat(50)).unwrap_or_else(|e| unreachable!("{e}"));
        assert!(!slow.matches(&format!("{}b", "a".repeat(60))));
    }
}
