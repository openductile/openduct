// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `summarise`: a text, or a list of texts, to a length and a style. What fits one piece is summarised in one call; a
//! longer input is cut into pieces at paragraph or sentence ends, each piece noted (a few at once), the notes combined
//! while they are still too long, and the summary written from them.

use serde_json::{Value as Json, json};

use super::{Asked, Call, Did, Job, at_most, result, shown, text_of};

/// The most pieces a long input is cut into: so many model calls, at most, before the summary.
pub(crate) const MAX_PIECES: usize = 40;
/// How many pieces are noted at once.
const AT_ONCE: usize = 4;
/// How many times notes still too long are combined again.
const MAX_ROUNDS: usize = 3;
/// What goes between two texts, or two pieces, set side by side.
const BETWEEN: &str = "\n\n---\n\n";

fn length(text: &str) -> usize {
    text.chars().count()
}

/// How the summary is to be: its length and its style, as the step says them.
fn how(size: Option<&str>, style: Option<&str>) -> String {
    let mut out = String::new();
    match size.map(str::trim) {
        None | Some("") => {}
        Some("short") => out.push_str(" in one or two sentences"),
        Some("medium") => out.push_str(" in a paragraph"),
        Some("long") => out.push_str(" in a few paragraphs"),
        Some(other) if other.starts_with("in ") => {
            out.push(' ');
            out.push_str(other);
        }
        Some(other) => {
            out.push_str(" in ");
            out.push_str(other);
        }
    }
    match style.map(str::trim) {
        None | Some("") => {}
        Some("bullets") => out.push_str(", as bullet points"),
        Some("paragraph" | "prose") => out.push_str(", as prose"),
        Some(other) => {
            out.push_str(", ");
            out.push_str(other);
        }
    }
    out
}

/// `text` cut into parts of at most `piece` characters, each cut at a paragraph's end, a line's, a sentence's or a
/// word's - in the second half of the part - where there is one.
fn split(text: &str, piece: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = text.trim();
    while length(rest) > piece {
        let end = rest.char_indices().nth(piece).map_or(rest.len(), |(i, _)| i);
        let window = rest.get(..end).unwrap_or(rest);
        let half = window.len() / 2;
        let cut = ["\n\n", "\n", ". ", " "]
            .iter()
            .find_map(|sep| window.rfind(sep).filter(|i| *i >= half).map(|i| i + sep.len()))
            .unwrap_or(end);
        out.push(rest.get(..cut).unwrap_or_default().trim().to_owned());
        rest = rest.get(cut..).unwrap_or_default().trim_start();
    }
    if !rest.is_empty() {
        out.push(rest.to_owned());
    }
    out
}

/// The texts as pieces of at most `piece` characters: short ones together, long ones cut.
fn pieces(texts: &[String], piece: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for text in texts {
        for part in split(text, piece) {
            if !current.is_empty() && length(&current) + BETWEEN.len() + length(&part) > piece {
                out.push(std::mem::take(&mut current));
            }
            if !current.is_empty() {
                current.push_str(BETWEEN);
            }
            current.push_str(&part);
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// A run of calls, one for each of `inputs`, at most a few at once; with what they cost.
async fn each(
    call: &Call<'_>,
    system: &(dyn Fn(usize) -> String + Sync),
    inputs: &[String],
    spent: &mut Spent,
) -> Result<Vec<Json>, Json> {
    let jobs: Vec<Job<'_, Result<Asked, String>>> = inputs
        .iter()
        .enumerate()
        .map(|(i, input)| {
            let mut fields = call.fields(&system(i), input, false);
            Box::pin(async move { call.ask(&mut fields).await }) as Job<'_, Result<Asked, String>>
        })
        .collect();
    let mut out = Vec::with_capacity(inputs.len());
    for asked in at_most(jobs, AT_ONCE).await {
        match asked {
            Ok(Asked::Answered(result)) => {
                spent.add(&result);
                out.push(result);
            }
            Ok(Asked::Shown(request)) => return Err(json!({ "shown": request })),
            Err(e) => return Err(json!({ "error": e })),
        }
    }
    Ok(out)
}

/// The tokens of every call of the step.
#[derive(Debug, Default)]
struct Spent {
    calls: u64,
    prompt: Option<u64>,
    completion: Option<u64>,
}

impl Spent {
    fn add(&mut self, result: &Json) {
        self.calls += 1;
        let add = |to: &mut Option<u64>, n: Option<u64>| {
            if let Some(n) = n {
                *to = Some(to.unwrap_or(0) + n);
            }
        };
        add(
            &mut self.prompt,
            result.pointer("/usage/prompt_tokens").and_then(Json::as_u64),
        );
        add(
            &mut self.completion,
            result.pointer("/usage/completion_tokens").and_then(Json::as_u64),
        );
    }
}

/// The step's failure or test-run answer, from what a run of calls gave back instead of answers.
fn stopped(said: &Json, pieces: usize) -> Result<Did, String> {
    if let Some(request) = said.get("shown") {
        return Ok(Did {
            result: shown(
                json!({ "summary": "(the summary, in a real run)", "pieces": pieces }),
                request.clone(),
            ),
            routes: Vec::new(),
        });
    }
    Err(said
        .get("error")
        .and_then(Json::as_str)
        .unwrap_or("the model gave no answer")
        .to_owned())
}

/// Summarises `texts`.
pub(crate) async fn summarise(
    call: &Call<'_>,
    texts: &[String],
    size: Option<&str>,
    style: Option<&str>,
    piece: usize,
) -> Result<Did, String> {
    let started = std::time::Instant::now();
    let total: usize = texts.iter().map(|t| length(t)).sum();
    if total == 0 {
        return Err("text: there is nothing to summarise".to_owned());
    }
    if total > MAX_PIECES * piece {
        return Err(format!(
            "text: {total} characters, more than {MAX_PIECES} pieces of {piece} (raise piece_size, or summarise \
             less at a time)"
        ));
    }
    let how = how(size, style);
    let cut = pieces(texts, piece);
    if cut.len() > MAX_PIECES {
        return Err(format!(
            "text: {} pieces of at most {piece} characters, more than {MAX_PIECES} (raise piece_size)",
            cut.len()
        ));
    }
    let mut spent = Spent::default();
    let parts = cut.len();
    let notes = if parts == 1 {
        cut
    } else {
        let note = move |i: usize| {
            format!(
                "This is part {} of {parts} of a longer text. Note what it says that a summary of the whole would \
                 need - its facts, names, numbers and decisions - in a few lines.",
                i + 1
            )
        };
        let mut notes: Vec<String> = match each(call, &note, &cut, &mut spent).await {
            Ok(results) => results.iter().map(text_of).collect(),
            Err(said) => return stopped(&said, parts),
        };
        // Notes still too long for one call are combined, a few at a time, until they fit.
        for _ in 0..MAX_ROUNDS {
            if length(&notes.join(BETWEEN)) <= piece {
                break;
            }
            let groups = pieces(&notes, piece);
            let combine = |_: usize| {
                "These are notes on parts of a longer text, in order. Combine them into shorter notes that keep \
                 what a summary of the whole would need."
                    .to_owned()
            };
            notes = match each(call, &combine, &groups, &mut spent).await {
                Ok(results) => results.iter().map(text_of).collect(),
                Err(said) => return stopped(&said, parts),
            };
        }
        notes
    };
    let joined = notes.join(BETWEEN);
    if length(&joined) > super::MAX_TEXT {
        return Err("text: the notes on its pieces are still too long to summarise".to_owned());
    }
    let system = if spent.calls > 0 {
        format!(
            "These are notes on the parts of a longer text, in order. Write a summary of the whole text from them{how}."
        )
    } else if texts.len() > 1 {
        format!(
            "Summarise the texts the user gives, set apart by lines of ---, together{how}. Keep to what they say."
        )
    } else {
        format!("Summarise the text the user gives{how}. Keep to what it says.")
    };
    let mut fields = call.fields(&system, &joined, false);
    let last = match call.ask(&mut fields).await? {
        Asked::Answered(result) => result,
        Asked::Shown(request) => {
            return stopped(&json!({ "shown": request }), parts);
        }
    };
    spent.add(&last);
    let mut answer = last;
    let total_tokens = spent.prompt.zip(spent.completion).map(|(p, c)| p + c);
    crate::apps::put(
        &mut answer,
        "usage",
        json!({ "prompt_tokens": spent.prompt, "completion_tokens": spent.completion, "total_tokens": total_tokens }),
    );
    if let Some(metrics) = answer.get_mut("metrics") {
        let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        crate::apps::put(metrics, "latency_ms", json!(ms));
        crate::apps::put(metrics, "calls", json!(spent.calls));
    }
    let summary = text_of(&answer);
    Ok(Did {
        result: result(json!({ "summary": summary.trim(), "pieces": parts }), &answer),
        routes: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_long_text_is_cut_where_its_sentences_end() {
        let text = "One two three. Four five six. Seven eight nine. Ten eleven twelve.";
        let parts = split(text, 30);
        assert_eq!(
            parts,
            [
                "One two three. Four five six.",
                "Seven eight nine.",
                "Ten eleven twelve."
            ]
        );
        assert!(parts.iter().all(|p| length(p) <= 30));
        let word = "x".repeat(25);
        assert_eq!(
            split(&word, 10),
            ["xxxxxxxxxx", "xxxxxxxxxx", "xxxxx"],
            "no place to cut: cut anyway"
        );
        assert_eq!(
            split("ééééé ééééé", 6),
            ["ééééé", "ééééé"],
            "counted in characters, not bytes"
        );
    }

    #[test]
    fn short_texts_go_together_into_a_piece() {
        let texts: Vec<String> = ["a".repeat(10), "b".repeat(10), "c".repeat(30)]
            .into_iter()
            .collect();
        let got = pieces(&texts, 30);
        assert_eq!(
            got,
            [
                format!("{}{BETWEEN}{}", "a".repeat(10), "b".repeat(10)),
                "c".repeat(30)
            ]
        );
    }

    #[test]
    fn the_summary_is_asked_for_as_the_step_says() {
        assert_eq!(
            how(Some("short"), Some("bullets")),
            " in one or two sentences, as bullet points"
        );
        assert_eq!(how(Some("3 sentences"), None), " in 3 sentences");
        assert_eq!(
            how(Some("in under 50 words"), Some("for a customer, warmly")),
            " in under 50 words, for a customer, warmly"
        );
        assert_eq!(how(None, None), "");
    }
}
