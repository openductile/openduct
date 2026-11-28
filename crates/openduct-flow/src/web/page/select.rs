// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! CSS selectors, to pick elements out of a page: names (`h1`, `*`), `#id`, `.class`, attributes (`[href]`,
//! `[rel=next]`, `~=`, `|=`, `^=`, `$=`, `*=`, with ` i` to ignore case), the combinators (descendant, `>`, `+`, `~`),
//! lists (`h1, h2`) and the pseudo-classes `:first-child`, `:last-child`, `:only-child`, `:first-of-type`,
//! `:last-of-type`, `:only-of-type`, `:nth-child()`, `:nth-last-child()`, `:nth-of-type()`, `:nth-last-of-type()`
//! (`2n+1`, `odd`, `even`, `3`), `:not()`, `:has()` (of what is inside), `:empty` and `:root`. Matching is bounded:
//! a selector is at most [`MAX_SELECTOR`] characters, and one page's matching at most [`BUDGET`] tests, so a
//! selector made to be slow fails, saying so, rather than running on.

use super::dom::Page;

/// The longest selector.
pub(crate) const MAX_SELECTOR: usize = 1_000;
/// The most tests of one element against one part of a selector, for one step's matching of a page.
pub(crate) const BUDGET: u64 = 20_000_000;
/// How deep `:not()` and `:has()` go inside each other.
const MAX_NESTING: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Combinator {
    /// Anywhere inside: `a b`.
    Inside,
    /// Directly inside: `a > b`.
    Child,
    /// Just after, beside: `a + b`.
    Next,
    /// After, beside: `a ~ b`.
    Later,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Test {
    Exists,
    Equals,
    /// A word among the value's words (`~=`).
    Word,
    /// The value, or it followed by `-` (`|=`).
    Lang,
    Starts,
    Ends,
    Contains,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Attribute {
    name: String,
    test: Test,
    value: String,
    any_case: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Pseudo {
    /// `:nth-child(an+b)` and its kin: (a, b), whether among elements of the same name, whether counted from the end.
    Nth(i64, i64, bool, bool),
    Only(bool),
    Not(Vec<Complex>),
    Has(Vec<Complex>),
    Empty,
    Root,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Compound {
    name: Option<String>,
    ids: Vec<String>,
    classes: Vec<String>,
    attributes: Vec<Attribute>,
    pseudos: Vec<Pseudo>,
}

/// A selector without commas: its compounds, the rightmost last, each with how it stands to the one before.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Complex(Vec<(Combinator, Compound)>);

/// A selector, read and checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Selector(Vec<Complex>);

/// The tests left for one step's matching: when none are, the matching stops.
#[derive(Debug)]
pub(crate) struct Budget(pub(crate) u64);

/// What a selector made to be slow is told.
const TOO_COSTLY: &str = "the selector takes too long to match on this page: make it simpler (fewer parts that \
                          look anywhere inside, or a class or an id to start from)";

struct Reader<'a> {
    text: &'a [u8],
    at: usize,
    depth: usize,
}

fn ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b >= 0x80
}

impl Reader<'_> {
    fn peek(&self) -> Option<u8> {
        self.text.get(self.at).copied()
    }

    fn spaces(&mut self) -> bool {
        let from = self.at;
        while self.peek().is_some_and(|b| b.is_ascii_whitespace()) {
            self.at += 1;
        }
        self.at > from
    }

    fn eat(&mut self, b: u8) -> bool {
        if self.peek() == Some(b) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    /// A name: letters, digits, `-`, `_`, and characters escaped with `\`.
    fn ident(&mut self) -> Result<String, String> {
        let mut out = Vec::new();
        while let Some(b) = self.peek() {
            if b == b'\\' {
                self.at += 1;
                let c = self.peek().ok_or("a `\\` at the end")?;
                out.push(c);
                self.at += 1;
            } else if ident_byte(b) {
                out.push(b);
                self.at += 1;
            } else {
                break;
            }
        }
        if out.is_empty() {
            return Err(format!("a name expected at {}", self.at + 1));
        }
        String::from_utf8(out).map_err(|_| "a name that is not text".to_owned())
    }

    fn quoted_or_ident(&mut self) -> Result<String, String> {
        match self.peek() {
            Some(q @ (b'"' | b'\'')) => {
                self.at += 1;
                let from = self.at;
                while self.peek().is_some_and(|b| b != q) {
                    self.at += 1;
                }
                let value = self.text.get(from..self.at).unwrap_or_default();
                if !self.eat(q) {
                    return Err("a quote that is not closed".to_owned());
                }
                String::from_utf8(value.to_vec()).map_err(|_| "a value that is not text".to_owned())
            }
            _ => self.ident(),
        }
    }

    fn list(&mut self) -> Result<Vec<Complex>, String> {
        let mut out = vec![self.complex()?];
        while self.eat(b',') {
            self.spaces();
            out.push(self.complex()?);
        }
        Ok(out)
    }

    fn complex(&mut self) -> Result<Complex, String> {
        self.spaces();
        let mut parts = vec![(Combinator::Inside, self.compound()?)];
        loop {
            let spaced = self.spaces();
            let combinator = match self.peek() {
                Some(b'>') => Combinator::Child,
                Some(b'+') => Combinator::Next,
                Some(b'~') => Combinator::Later,
                Some(b',' | b')') | None => break,
                Some(_) if spaced => Combinator::Inside,
                Some(b) => return Err(format!("{:?} is not expected at {}", char::from(b), self.at + 1)),
            };
            if combinator != Combinator::Inside {
                self.at += 1;
                self.spaces();
            }
            parts.push((combinator, self.compound()?));
            if parts.len() > 32 {
                return Err("at most 32 parts".to_owned());
            }
        }
        Ok(Complex(parts))
    }

    fn compound(&mut self) -> Result<Compound, String> {
        let mut c = Compound::default();
        let star = self.eat(b'*');
        if !star && (self.peek().is_some_and(ident_byte) || self.peek() == Some(b'\\')) {
            c.name = Some(self.ident()?.to_ascii_lowercase());
        }
        let mut any = star || c.name.is_some();
        loop {
            match self.peek() {
                Some(b'#') => {
                    self.at += 1;
                    c.ids.push(self.ident()?);
                }
                Some(b'.') => {
                    self.at += 1;
                    c.classes.push(self.ident()?);
                }
                Some(b'[') => {
                    self.at += 1;
                    c.attributes.push(self.attribute()?);
                }
                Some(b':') => {
                    self.at += 1;
                    c.pseudos.push(self.pseudo()?);
                }
                _ => break,
            }
            any = true;
        }
        if !any {
            return Err(match self.peek() {
                Some(b) => format!("{:?} is not expected at {}", char::from(b), self.at + 1),
                None => "the selector ends too soon".to_owned(),
            });
        }
        Ok(c)
    }

    fn attribute(&mut self) -> Result<Attribute, String> {
        self.spaces();
        let name = self.ident()?.to_ascii_lowercase();
        self.spaces();
        let test = match self.peek() {
            Some(b']') => {
                self.at += 1;
                return Ok(Attribute {
                    name,
                    test: Test::Exists,
                    value: String::new(),
                    any_case: false,
                });
            }
            Some(b'=') => Test::Equals,
            Some(b'~') => Test::Word,
            Some(b'|') => Test::Lang,
            Some(b'^') => Test::Starts,
            Some(b'$') => Test::Ends,
            Some(b'*') => Test::Contains,
            None => return Err(format!("[{name}: `]` expected")),
            _ => return Err(format!("[{name}: =, ~=, |=, ^=, $= or *= expected")),
        };
        self.at += 1;
        if test != Test::Equals && !self.eat(b'=') {
            return Err(format!("[{name}: `=` expected"));
        }
        self.spaces();
        let value = self.quoted_or_ident()?;
        self.spaces();
        let any_case = self.eat(b'i') || self.eat(b'I');
        if !any_case {
            let _ = self.eat(b's') || self.eat(b'S');
        }
        self.spaces();
        if !self.eat(b']') {
            return Err(format!("[{name}: `]` expected"));
        }
        Ok(Attribute {
            name,
            test,
            value,
            any_case,
        })
    }

    fn pseudo(&mut self) -> Result<Pseudo, String> {
        let name = self.ident()?.to_ascii_lowercase();
        let nth = |of_type: bool, from_end: bool, r: &mut Self| -> Result<Pseudo, String> {
            let (a, b) = r.argument().and_then(|t| nth(&t))?;
            Ok(Pseudo::Nth(a, b, of_type, from_end))
        };
        Ok(match name.as_str() {
            "first-child" => Pseudo::Nth(0, 1, false, false),
            "last-child" => Pseudo::Nth(0, 1, false, true),
            "first-of-type" => Pseudo::Nth(0, 1, true, false),
            "last-of-type" => Pseudo::Nth(0, 1, true, true),
            "only-child" => Pseudo::Only(false),
            "only-of-type" => Pseudo::Only(true),
            "empty" => Pseudo::Empty,
            "root" => Pseudo::Root,
            "nth-child" => nth(false, false, self)?,
            "nth-last-child" => nth(false, true, self)?,
            "nth-of-type" => nth(true, false, self)?,
            "nth-last-of-type" => nth(true, true, self)?,
            "not" | "has" => {
                if self.depth >= MAX_NESTING {
                    return Err(format!(":{name}: at most {MAX_NESTING} inside each other"));
                }
                if !self.eat(b'(') {
                    return Err(format!(":{name}: `(` expected"));
                }
                self.depth += 1;
                let inner = self.list()?;
                self.depth -= 1;
                self.spaces();
                if !self.eat(b')') {
                    return Err(format!(":{name}: `)` expected"));
                }
                if name == "not" {
                    Pseudo::Not(inner)
                } else {
                    Pseudo::Has(inner)
                }
            }
            other => {
                return Err(format!(
                    ":{other} is not a pseudo-class this selector knows: first-child, last-child, nth-child(), \
                     nth-of-type(), not(), has(), empty ..."
                ));
            }
        })
    }

    /// The text between `(` and `)`.
    fn argument(&mut self) -> Result<String, String> {
        if !self.eat(b'(') {
            return Err("`(` expected".to_owned());
        }
        let from = self.at;
        while self.peek().is_some_and(|b| b != b')') {
            self.at += 1;
        }
        let inside = String::from_utf8_lossy(self.text.get(from..self.at).unwrap_or_default()).into_owned();
        if !self.eat(b')') {
            return Err("`)` expected".to_owned());
        }
        Ok(inside)
    }
}

/// `an+b`, `odd`, `even`, `3`.
fn nth(text: &str) -> Result<(i64, i64), String> {
    let t: String = text
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_ascii_lowercase();
    let wrong = || format!("{text:?} is not an+b, odd, even or a number");
    match t.as_str() {
        "odd" => return Ok((2, 1)),
        "even" => return Ok((2, 0)),
        _ => {}
    }
    let Some(n) = t.find('n') else {
        return t.parse::<i64>().map(|b| (0, b)).map_err(|_| wrong());
    };
    let a = match t.get(..n).unwrap_or_default() {
        "" | "+" => 1,
        "-" => -1,
        a => a.parse::<i64>().map_err(|_| wrong())?,
    };
    let b = match t.get(n + 1..).unwrap_or_default() {
        "" => 0,
        b => b
            .strip_prefix('+')
            .unwrap_or(b)
            .parse::<i64>()
            .map_err(|_| wrong())?,
    };
    if a.abs() > 1_000_000 || b.abs() > 1_000_000 {
        return Err(wrong());
    }
    Ok((a, b))
}

impl Selector {
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        if text.is_empty() {
            return Err("a selector, as h1, .price or a[href]".to_owned());
        }
        if text.len() > MAX_SELECTOR {
            return Err(format!("a selector of at most {MAX_SELECTOR} characters"));
        }
        let mut r = Reader {
            text: text.as_bytes(),
            at: 0,
            depth: 0,
        };
        let list = r.list()?;
        r.spaces();
        if let Some(b) = r.peek() {
            return Err(format!("{:?} is not expected at {}", char::from(b), r.at + 1));
        }
        Ok(Self(list))
    }

    /// The elements inside `scope` it matches, in the document's order; at most `most`.
    pub(crate) fn select(
        &self,
        page: &Page,
        scope: usize,
        most: usize,
        budget: &mut Budget,
    ) -> Result<Vec<usize>, String> {
        let mut out = Vec::new();
        for id in page.within(scope).skip(1) {
            if page.name(id).is_none() {
                continue;
            }
            if self.matches(page, id, budget)? {
                out.push(id);
                if out.len() >= most {
                    break;
                }
            }
        }
        Ok(out)
    }

    pub(crate) fn matches(&self, page: &Page, id: usize, budget: &mut Budget) -> Result<bool, String> {
        any_of(&self.0, page, id, budget)
    }
}

fn any_of(list: &[Complex], page: &Page, id: usize, budget: &mut Budget) -> Result<bool, String> {
    for c in list {
        if complex(page, id, &c.0, budget)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether `id` matches the parts of a selector up to and with the last one, read from the right.
fn complex(
    page: &Page,
    id: usize,
    parts: &[(Combinator, Compound)],
    budget: &mut Budget,
) -> Result<bool, String> {
    let Some(((combinator, last), rest)) = parts.split_last() else {
        return Ok(true);
    };
    if !compound(page, id, last, budget)? {
        return Ok(false);
    }
    if rest.is_empty() {
        return Ok(true);
    }
    match combinator {
        Combinator::Child => match page.parent(id).filter(|p| page.name(*p).is_some()) {
            Some(p) => complex(page, p, rest, budget),
            None => Ok(false),
        },
        Combinator::Inside => {
            for a in page.ancestors(id) {
                if complex(page, a, rest, budget)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Combinator::Next => match siblings_before(page, id, budget)?.last() {
            Some(s) => complex(page, *s, rest, budget),
            None => Ok(false),
        },
        Combinator::Later => {
            for s in siblings_before(page, id, budget)?.iter().rev() {
                if complex(page, *s, rest, budget)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

/// Takes `n` tests from the budget.
fn charge(budget: &mut Budget, n: usize) -> Result<(), String> {
    let n = u64::try_from(n).unwrap_or(u64::MAX).max(1);
    if budget.0 < n {
        budget.0 = 0;
        return Err(TOO_COSTLY.to_owned());
    }
    budget.0 -= n;
    Ok(())
}

/// The elements before `id` beside it, in order.
fn siblings_before(page: &Page, id: usize, budget: &mut Budget) -> Result<Vec<usize>, String> {
    let Some(parent) = page.parent(id) else {
        return Ok(Vec::new());
    };
    charge(budget, page.children(parent).len())?;
    Ok(page.elements(parent).take_while(|s| *s != id).collect())
}

fn attribute_holds(page: &Page, id: usize, a: &Attribute) -> bool {
    let Some(value) = page.attr(id, &a.name) else {
        return false;
    };
    let (value, want) = if a.any_case {
        (value.to_lowercase(), a.value.to_lowercase())
    } else {
        (value.to_owned(), a.value.clone())
    };
    match a.test {
        Test::Exists => true,
        Test::Equals => value == want,
        Test::Word => !want.is_empty() && value.split_whitespace().any(|w| w == want),
        Test::Lang => value == want || value.starts_with(&format!("{want}-")),
        Test::Starts => !want.is_empty() && value.starts_with(&want),
        Test::Ends => !want.is_empty() && value.ends_with(&want),
        Test::Contains => !want.is_empty() && value.contains(&want),
    }
}

/// Whether `id` is the (a·n+b)th element among its siblings (of its own name, when `of_type`).
fn nth_holds(
    page: &Page,
    id: usize,
    (a, b): (i64, i64),
    (of_type, from_end): (bool, bool),
    budget: &mut Budget,
) -> Result<bool, String> {
    let Some(parent) = page.parent(id) else {
        return Ok(false);
    };
    charge(budget, page.children(parent).len())?;
    let name = page.name(id);
    let mut beside: Vec<usize> = page
        .elements(parent)
        .filter(|s| !of_type || page.name(*s) == name)
        .collect();
    if from_end {
        beside.reverse();
    }
    let Some(position) = beside.iter().position(|s| *s == id) else {
        return Ok(false);
    };
    let index = i64::try_from(position).unwrap_or(i64::MAX).saturating_add(1);
    if a == 0 {
        return Ok(index == b);
    }
    let n = (index - b) / a;
    Ok(n >= 0 && a * n + b == index)
}

fn compound(page: &Page, id: usize, c: &Compound, budget: &mut Budget) -> Result<bool, String> {
    charge(budget, 1)?;
    let Some(name) = page.name(id) else {
        return Ok(false);
    };
    if c.name.as_deref().is_some_and(|n| n != name) {
        return Ok(false);
    }
    if !c.ids.iter().all(|i| page.attr(id, "id") == Some(i.as_str())) {
        return Ok(false);
    }
    if !c.classes.is_empty() {
        let classes = page.attr(id, "class").unwrap_or_default();
        if !c
            .classes
            .iter()
            .all(|k| classes.split_whitespace().any(|w| w == k))
        {
            return Ok(false);
        }
    }
    if !c.attributes.iter().all(|a| attribute_holds(page, id, a)) {
        return Ok(false);
    }
    for p in &c.pseudos {
        let holds = match p {
            Pseudo::Nth(a, b, of_type, from_end) => {
                nth_holds(page, id, (*a, *b), (*of_type, *from_end), budget)?
            }
            Pseudo::Only(of_type) => {
                nth_holds(page, id, (0, 1), (*of_type, false), budget)?
                    && nth_holds(page, id, (0, 1), (*of_type, true), budget)?
            }
            Pseudo::Empty => page
                .children(id)
                .iter()
                .all(|c| page.name(*c).is_none() && page.text_node(*c).is_none_or(str::is_empty)),
            Pseudo::Root => page.parent(id).is_some_and(|p| page.name(p).is_none()),
            Pseudo::Not(list) => !any_of(list, page, id, budget)?,
            Pseudo::Has(list) => {
                let mut found = false;
                for inner in page.within(id).skip(1) {
                    if page.name(inner).is_some() && any_of(list, page, inner, budget)? {
                        found = true;
                        break;
                    }
                }
                found
            }
        };
        if !holds {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: &str = r#"<html><body><div id="shop" class="shop wide">
        <h1>Shop</h1>
        <ul class="items">
          <li class="item" data-sku="CAP-1"><a href="/cap">Cap</a><span class="price">24</span></li>
          <li class="item sale" data-sku="TOTE-2"><a href="/tote" rel="next noopener">Tote</a><span class="price">12</span></li>
          <li class="item" data-sku="MUG-3"><a href="/mug">Mug</a><span class="price">9</span><em>new</em></li>
        </ul>
        <p lang="en-GB">Prices in pounds.</p><p></p>
        </div></body></html>"#;

    fn texts(selector: &str) -> Vec<String> {
        let page = Page::parse(PAGE, false);
        let s = Selector::parse(selector).unwrap();
        s.select(&page, 0, 100, &mut Budget(BUDGET))
            .unwrap()
            .into_iter()
            .map(|id| page.text_of(id, &|_| false))
            .collect()
    }

    #[test]
    fn selectors_pick_what_a_browser_picks() {
        assert_eq!(texts("h1"), ["Shop"]);
        assert_eq!(texts(".item .price"), ["24", "12", "9"]);
        assert_eq!(texts("#shop > ul > li.sale > a"), ["Tote"]);
        assert_eq!(texts("li:first-child a, li:last-child a"), ["Cap", "Mug"]);
        assert_eq!(texts("li:nth-child(2n+1) .price"), ["24", "9"]);
        assert_eq!(texts("li:nth-child(even) a"), ["Tote"]);
        assert_eq!(texts("li:nth-last-child(1) a"), ["Mug"]);
        assert_eq!(texts("[data-sku^=TOTE] a"), ["Tote"]);
        assert_eq!(texts("[data-sku$='-3'] .price"), ["9"]);
        assert_eq!(texts("[data-sku*=\"U\"] a"), ["Mug"]);
        assert_eq!(texts("a[rel~=next]"), ["Tote"]);
        assert_eq!(texts("p[lang|=en]"), ["Prices in pounds."]);
        assert_eq!(texts("a[href=\"/CAP\" i]"), ["Cap"]);
        assert_eq!(texts("li:not(.sale) a"), ["Cap", "Mug"]);
        assert_eq!(texts("li:has(em) a"), ["Mug"]);
        assert_eq!(texts("li.item + li a"), ["Tote", "Mug"]);
        assert_eq!(texts("li.sale ~ li a"), ["Mug"]);
        assert_eq!(texts("p:empty").len(), 1);
        assert_eq!(texts("span:only-of-type"), ["24", "12", "9"]);
        assert_eq!(texts("html:root > body h1"), ["Shop"]);
        assert_eq!(texts("*.wide > h1"), ["Shop"]);
        assert!(texts("table td").is_empty());
    }

    #[test]
    fn a_wrong_selector_is_told_plainly() {
        for (wrong, says) in [
            ("", "a selector"),
            ("li:hover", "not a pseudo-class"),
            ("a[href", "`]` expected"),
            ("a[href~x]", "`=` expected"),
            ("div >", "ends too soon"),
            ("p)", "not expected"),
            ("li:nth-child(x)", "not an+b"),
            (":not(:not(:not(:not(:not(p)))))", "at most 4"),
        ] {
            let e = Selector::parse(wrong).unwrap_err();
            assert!(e.contains(says), "{wrong}: {e}");
        }
        assert!(
            Selector::parse(&"a ".repeat(600))
                .unwrap_err()
                .contains("at most")
        );
    }

    #[test]
    fn a_selector_made_to_be_slow_stops_at_its_budget() {
        let deep = "<div>".repeat(200) + &"<p>x</p>".repeat(200);
        let page = Page::parse(&deep, false);
        let s = Selector::parse("span div div div div div div div div div div p").unwrap();
        let e = s.select(&page, 0, 10, &mut Budget(100_000)).unwrap_err();
        assert!(e.contains("too long"));
        assert_eq!(nth("-n+3"), Ok((-1, 3)));
        assert_eq!(nth(" 3 "), Ok((0, 3)));
    }
}
