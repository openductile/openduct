// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Limits on an expression's text, checked before it is parsed. The parser and the evaluator recurse once per level of
//! an expression, and a thread that recurses past its stack does not fail - the whole process stops. Measured on
//! `cel` 0.16 (2026-11-16): a release build on a 2 MiB thread stack handles 2,048 nested brackets or lists and chains
//! of 512 operators; a debug build fails far earlier. These limits leave a wide margin, and no real expression comes
//! near them.

/// The longest expression taken, in bytes.
pub const MAX_LENGTH: usize = 4096;
/// The deepest nesting of brackets - `(`, `[` and `{`.
pub const MAX_NESTING: usize = 32;
/// The most operators, dots and brackets in one expression: each can add a level the parser recurses through.
pub const MAX_STRUCTURE: usize = 256;

/// Whether `source` is within the limits; why not, if it is not.
pub(crate) fn check(source: &str) -> Result<(), String> {
    if source.len() > MAX_LENGTH {
        return Err(format!(
            "an expression of {} bytes: at most {MAX_LENGTH}",
            source.len()
        ));
    }
    let bytes = source.as_bytes();
    let (mut depth, mut deepest, mut structure) = (0usize, 0usize, 0usize);
    let mut i = 0;
    while let Some(&b) = bytes.get(i) {
        match b {
            b'"' | b'\'' => i = skip_string(bytes, i),
            b'(' | b'[' | b'{' => {
                depth += 1;
                deepest = deepest.max(depth);
                structure += 1;
            }
            b')' | b']' | b'}' => depth = depth.saturating_sub(1),
            b'+' | b'-' | b'*' | b'/' | b'%' | b'<' | b'>' | b'=' | b'!' | b'&' | b'|' | b'?' | b':'
            | b'.' => {
                structure += 1;
            }
            _ => {}
        }
        i += 1;
    }
    if deepest > MAX_NESTING {
        return Err(format!("brackets nested {deepest} deep: at most {MAX_NESTING}"));
    }
    if structure > MAX_STRUCTURE {
        return Err(format!(
            "an expression of {structure} operators and brackets: at most {MAX_STRUCTURE} - split it into named steps \
             (let)"
        ));
    }
    Ok(())
}

/// The index of the last byte of the string literal starting at `start` (a quote): single, double and triple-quoted,
/// with backslash escapes. An unterminated string runs to the end; the parser then reports it.
fn skip_string(bytes: &[u8], start: usize) -> usize {
    let Some(&quote) = bytes.get(start) else {
        return start;
    };
    let triple = bytes.get(start + 1) == Some(&quote) && bytes.get(start + 2) == Some(&quote);
    let mut i = start + if triple { 3 } else { 1 };
    while let Some(&b) = bytes.get(i) {
        if b == b'\\' {
            i += 2;
            continue;
        }
        if b == quote {
            if !triple {
                return i;
            }
            if bytes.get(i + 1) == Some(&quote) && bytes.get(i + 2) == Some(&quote) {
                return i + 2;
            }
        }
        i += 1;
    }
    bytes.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_expressions_pass_and_extreme_ones_are_refused_with_the_limit() {
        assert!(check("status >= 500 ? 'error' : 'ok'").is_ok());
        assert!(check(&format!("{}1{}", "(".repeat(32), ")".repeat(32))).is_ok());
        let deep = check(&format!("{}1{}", "[".repeat(33), "]".repeat(33))).unwrap_err();
        assert!(deep.contains("33 deep"), "{deep}");
        let long = check(&"1+".repeat(300)).unwrap_err();
        assert!(long.contains("at most 256"), "{long}");
        assert!(check(&"x".repeat(MAX_LENGTH + 1)).unwrap_err().contains("bytes"));
    }

    #[test]
    fn what_is_inside_text_does_not_count() {
        let brackets = format!("'{}' + \"{}\"", "(".repeat(100), ")+".repeat(200));
        assert!(check(&brackets).is_ok());
        assert!(check(&format!("'''{}''' == x", "[".repeat(100))).is_ok());
        assert!(
            check(r"'it\'s ((((' == x").is_ok(),
            "an escaped quote does not end the text"
        );
        // Text that never ends is the parser's to report; nothing after it is counted, nothing is read past the end.
        assert!(check("'((((((((").is_ok());
    }
}
