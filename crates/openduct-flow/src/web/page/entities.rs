// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Character references, as pages write them: `&amp;`, `&eacute;`, `&#233;`, `&#xE9;`. The named ones are those of
//! HTML 4 (the ones pages use, the Latin-1 letters and signs among them) and a few later ones; a name not known is
//! left as it is written.

/// The Latin-1 references, U+00A0 to U+00FF in order.
const LATIN1: [&str; 96] = [
    "nbsp", "iexcl", "cent", "pound", "curren", "yen", "brvbar", "sect", "uml", "copy", "ordf", "laquo",
    "not", "shy", "reg", "macr", "deg", "plusmn", "sup2", "sup3", "acute", "micro", "para", "middot",
    "cedil", "sup1", "ordm", "raquo", "frac14", "frac12", "frac34", "iquest", "Agrave", "Aacute", "Acirc",
    "Atilde", "Auml", "Aring", "AElig", "Ccedil", "Egrave", "Eacute", "Ecirc", "Euml", "Igrave", "Iacute",
    "Icirc", "Iuml", "ETH", "Ntilde", "Ograve", "Oacute", "Ocirc", "Otilde", "Ouml", "times", "Oslash",
    "Ugrave", "Uacute", "Ucirc", "Uuml", "Yacute", "THORN", "szlig", "agrave", "aacute", "acirc", "atilde",
    "auml", "aring", "aelig", "ccedil", "egrave", "eacute", "ecirc", "euml", "igrave", "iacute", "icirc",
    "iuml", "eth", "ntilde", "ograve", "oacute", "ocirc", "otilde", "ouml", "divide", "oslash", "ugrave",
    "uacute", "ucirc", "uuml", "yacute", "thorn", "yuml",
];

/// The Greek letters, capital (from U+0391) and small (from U+03B1); `""` where the alphabet has a gap.
const GREEK: [&str; 25] = [
    "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta", "Iota", "Kappa", "Lambda", "Mu",
    "Nu", "Xi", "Omicron", "Pi", "Rho", "", "Sigma", "Tau", "Upsilon", "Phi", "Chi", "Psi", "Omega",
];

/// The other named references, by their code point.
const OTHERS: &[(&str, u32)] = &[
    ("quot", 34),
    ("amp", 38),
    ("apos", 39),
    ("lt", 60),
    ("gt", 62),
    ("OElig", 338),
    ("oelig", 339),
    ("Scaron", 352),
    ("scaron", 353),
    ("Yuml", 376),
    ("fnof", 402),
    ("circ", 710),
    ("tilde", 732),
    ("sigmaf", 962),
    ("thetasym", 977),
    ("upsih", 978),
    ("piv", 982),
    ("ensp", 8194),
    ("emsp", 8195),
    ("thinsp", 8201),
    ("ZeroWidthSpace", 8203),
    ("zwnj", 8204),
    ("zwj", 8205),
    ("lrm", 8206),
    ("rlm", 8207),
    ("dash", 8208),
    ("hyphen", 8208),
    ("ndash", 8211),
    ("mdash", 8212),
    ("horbar", 8213),
    ("lsquo", 8216),
    ("rsquo", 8217),
    ("sbquo", 8218),
    ("ldquo", 8220),
    ("rdquo", 8221),
    ("bdquo", 8222),
    ("dagger", 8224),
    ("Dagger", 8225),
    ("bull", 8226),
    ("hellip", 8230),
    ("mldr", 8230),
    ("permil", 8240),
    ("prime", 8242),
    ("Prime", 8243),
    ("lsaquo", 8249),
    ("rsaquo", 8250),
    ("oline", 8254),
    ("frasl", 8260),
    ("euro", 8364),
    ("image", 8465),
    ("weierp", 8472),
    ("real", 8476),
    ("trade", 8482),
    ("alefsym", 8501),
    ("larr", 8592),
    ("uarr", 8593),
    ("rarr", 8594),
    ("darr", 8595),
    ("harr", 8596),
    ("crarr", 8629),
    ("lArr", 8656),
    ("uArr", 8657),
    ("rArr", 8658),
    ("dArr", 8659),
    ("hArr", 8660),
    ("forall", 8704),
    ("part", 8706),
    ("exist", 8707),
    ("empty", 8709),
    ("nabla", 8711),
    ("isin", 8712),
    ("notin", 8713),
    ("ni", 8715),
    ("prod", 8719),
    ("sum", 8721),
    ("minus", 8722),
    ("lowast", 8727),
    ("radic", 8730),
    ("prop", 8733),
    ("infin", 8734),
    ("ang", 8736),
    ("and", 8743),
    ("or", 8744),
    ("cap", 8745),
    ("cup", 8746),
    ("int", 8747),
    ("there4", 8756),
    ("sim", 8764),
    ("cong", 8773),
    ("asymp", 8776),
    ("ne", 8800),
    ("equiv", 8801),
    ("le", 8804),
    ("ge", 8805),
    ("sub", 8834),
    ("sup", 8835),
    ("nsub", 8836),
    ("sube", 8838),
    ("supe", 8839),
    ("oplus", 8853),
    ("otimes", 8855),
    ("perp", 8869),
    ("sdot", 8901),
    ("lceil", 8968),
    ("rceil", 8969),
    ("lfloor", 8970),
    ("rfloor", 8971),
    ("loz", 9674),
    ("spades", 9824),
    ("clubs", 9827),
    ("hearts", 9829),
    ("diams", 9830),
    ("starf", 9733),
    ("star", 9734),
    ("check", 10003),
    ("cross", 10007),
    ("lang", 10216),
    ("rang", 10217),
];

/// What Windows-1252 puts at 0x80 to 0x9F, which `&#146;` and its like mean on the web.
const WINDOWS_1252: [u32; 32] = [
    0x20AC, 0x81, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6, 0x2030, 0x0160, 0x2039, 0x0152,
    0x8D, 0x017D, 0x8F, 0x90, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014, 0x02DC, 0x2122, 0x0161,
    0x203A, 0x0153, 0x9D, 0x017E, 0x0178,
];

/// The references pages write without their `;`: read so in text, when no letter or digit follows.
const BARE: &[&str] = &["amp", "lt", "gt", "quot", "nbsp", "copy", "reg"];

/// The character a reference's name stands for.
fn named(name: &str) -> Option<char> {
    if let Some(i) = LATIN1.iter().position(|n| *n == name) {
        return char::from_u32(0xA0 + u32::try_from(i).ok()?);
    }
    for (i, n) in GREEK.iter().enumerate().filter(|(_, n)| !n.is_empty()) {
        let i = u32::try_from(i).ok()?;
        if *n == name {
            return char::from_u32(0x391 + i);
        }
        if n.to_ascii_lowercase() == name {
            return char::from_u32(0x3B1 + i);
        }
    }
    OTHERS
        .iter()
        .find(|(n, _)| *n == name)
        .and_then(|(_, c)| char::from_u32(*c))
}

/// The character of a numeric reference's code (`&#233;`): what a browser makes of it.
fn numbered(code: u32) -> char {
    match code {
        0 => '\u{fffd}',
        0x80..=0x9F => WINDOWS_1252
            .get(usize::try_from(code - 0x80).unwrap_or(0))
            .and_then(|c| char::from_u32(*c))
            .unwrap_or('\u{fffd}'),
        _ => char::from_u32(code).unwrap_or('\u{fffd}'),
    }
}

/// The reference at the start of `text` (which starts with `&`): its character and how many bytes it took. In an
/// attribute (`in_attribute`) a name is read only with its `;`, so `?a=1&copy=2` in an address stays as it is.
pub(crate) fn reference(text: &str, in_attribute: bool) -> Option<(char, usize)> {
    let body = text.get(1..)?;
    if let Some(number) = body.strip_prefix('#') {
        let (hex, digits) = match number.strip_prefix(['x', 'X']) {
            Some(h) => (true, h),
            None => (false, number),
        };
        let len = digits
            .bytes()
            .take_while(|b| {
                if hex {
                    b.is_ascii_hexdigit()
                } else {
                    b.is_ascii_digit()
                }
            })
            .count();
        if len == 0 {
            return None;
        }
        let shown = digits.get(..len)?;
        let code =
            u32::from_str_radix(shown.get(..len.min(8))?, if hex { 16 } else { 10 }).unwrap_or(0x110000);
        let mut used = 1 + 1 + usize::from(hex) + len;
        if digits.as_bytes().get(len) == Some(&b';') {
            used += 1;
        }
        return Some((numbered(code), used));
    }
    let len = body
        .bytes()
        .take(32)
        .take_while(u8::is_ascii_alphanumeric)
        .count();
    let name = body.get(..len)?;
    if len > 0 && body.as_bytes().get(len) == Some(&b';') {
        return named(name).map(|c| (c, len + 2));
    }
    if in_attribute {
        return None;
    }
    // Without the `;`: only the few names pages write so, and only when the name ends there.
    BARE.iter()
        .filter(|b| name.starts_with(**b))
        .max_by_key(|b| b.len())
        .filter(|b| b.len() == len)
        .and_then(|b| named(b).map(|c| (c, b.len() + 1)))
}

/// `text` with its references read.
pub(crate) fn decode(text: &str, in_attribute: bool) -> String {
    if !text.contains('&') {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(rest.get(..at).unwrap_or_default());
        rest = rest.get(at..).unwrap_or_default();
        match reference(rest, in_attribute) {
            Some((c, used)) => {
                out.push(c);
                rest = rest.get(used..).unwrap_or_default();
            }
            None => {
                out.push('&');
                rest = rest.get(1..).unwrap_or_default();
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references_read_as_a_browser_reads_them() {
        assert_eq!(
            decode(
                "Caf&eacute; &amp; cr&egrave;me &mdash; &euro;5 &#233;&#xE9;&#146;",
                false
            ),
            "Café & crème — €5 éé’"
        );
        assert_eq!(decode("&Omega;&omega;&alpha;&Sigma;&sigmaf;", false), "ΩωαΣς");
        assert_eq!(
            decode("a &amp b &copy 2026 &ampx &unknown; &", false),
            "a & b © 2026 &ampx &unknown; &"
        );
        assert_eq!(decode("?a=1&copy=2&amp;b=3", true), "?a=1&copy=2&b=3");
        assert_eq!(
            decode("&#0;&#x110000;&#99999999999;", false),
            "\u{fffd}\u{fffd}\u{fffd}"
        );
        assert_eq!(decode("&nbsp;&yuml;&#;&#x;", false), "\u{a0}ÿ&#;&#x;");
    }
}
