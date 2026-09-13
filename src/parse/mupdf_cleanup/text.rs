//! Ordered generic repairs from clean_ocr_text.py, without book-specific guesses.

use anyhow::{Context, Result};
use fancy_regex::{Captures, Regex, RegexBuilder};
use serde::Serialize;

const TOKEN_EDGE_PUNCTUATION: &str = "\"'“”‘’.,;:!?()[]…*_—–-";

// Order follows the supplied script: possessive repairs precede quote pairing,
// and punctuation normalization must not hide spaced echo-stutter evidence.
const GENERIC_PASSES: &[(&str, &str, &str)] = &[
    ("spaced_ellipsis", r"\.(?: \.){2,}", "..."),
    (
        "spaced_contraction",
        r"(\w) ' (s|t|re|ve|ll|d|m|em|clock)\b",
        "${1}'${2}",
    ),
    (
        "uppercase_contraction",
        r"\b([A-Z]+) ' (S|VE|T|RE|LL|D|M)\b",
        "${1}'${2}",
    ),
    (
        "chapter_dash_spacing",
        r"(CHAPTER \d+) -(\w)",
        "${1} - ${2}",
    ),
    ("spaced_hyphen", r"(\w) -(\w)", "${1}-${2}"),
    ("detached_wh", r"\b([Ww]) h(?=[a-z])", "${1}h"),
    ("quoted_word_plural", r#"(") ' s\b"#, "${1}'s"),
    (
        "nested_single_quote",
        r#"(?<=")' ([^']*?) '(?=[\s.,])"#,
        "'${1}'",
    ),
    ("elided_apostrophe", r"\b([dDlL]) ' (?=\w)", "${1}'"),
    ("plural_possessive", r"(?<!' )\b(\w+s) ' (?=\w)", "${1}' "),
    (
        "possessive_before_quote",
        r#"\b(\w+s) ' (?=["…])"#,
        "${1}' ",
    ),
    ("g_dropping", r"\b(diggin|struttin) ' ", "${1}' "),
    ("opening_single_quote", r"(?<=[,?] )' (?=[A-Z])", "'"),
    (
        "single_quote_pair",
        r"(?<=\s)' ([^']{1,60}?) '(?=[\s.,;:!?)—]|$)",
        "'${1}'",
    ),
    ("quoted_suffix", r#"" -(\w)"#, "\"-${1}"),
    ("space_before_punctuation", r" +([,;:!?])", "${1}"),
    ("space_before_period", r" +\.(?!\.)", "."),
    ("opening_parenthesis_padding", r"\( +", "("),
    ("closing_parenthesis_padding", r" +\)", ")"),
    ("repeated_space", r"(?<=\S)  +(?=\S)", " "),
    (
        "wrapped_hyphenation",
        r"([A-Za-z])- (?!(?:and|or|nor|to)\b)([a-z]\w*)",
        "${1}${2}",
    ),
];

/// Full paragraph snapshots preserve every applied pass in execution order.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Repair {
    pub(super) rule: &'static str,
    pub(super) before: String,
    pub(super) after: String,
}

/// A dropped paragraph still carries its original text in the repair record.
pub(super) struct CleanedText {
    pub(super) text: String,
    pub(super) dropped: bool,
    pub(super) repairs: Vec<Repair>,
}

struct Substitution {
    rule: &'static str,
    regex: Regex,
    replacement: &'static str,
}

/// Reuse compiled expressions across all reconstructed paragraphs in a document.
pub(super) struct TextCleaner {
    generic: Vec<Substitution>,
    structural: Regex,
    comma_echo: Regex,
    period_echo: Regex,
    quote_pairs: Regex,
    possessive_echo: Regex,
    quote_echo: Regex,
}

impl TextCleaner {
    /// Compilation failures retain the responsible rule before any text changes.
    pub(super) fn new(backtrack_limit: usize) -> Result<Self> {
        // The configured budget applies to every rule; malformed input fails
        // with the rule name rather than consuming unbounded backtracking work.
        let compile = |rule, pattern| compile(rule, pattern, backtrack_limit);
        let generic = GENERIC_PASSES
            .iter()
            .map(|&(rule, pattern, replacement)| {
                Ok(Substitution {
                    rule,
                    regex: compile(rule, pattern)?,
                    replacement,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            generic,
            structural: compile("junk_paragraph", r"(?i)\b(chapter|part|book)\b")?,
            comma_echo: compile("comma_echo", r"(\w+) ?, ?(\w) ?,(?= |\n|$)")?,
            period_echo: compile("period_echo", r"(\w+) ?\. ?(\w) ?\.(?= |\n|$)")?,
            quote_pairs: compile("double_quote_pair", r#""\s*([^"]*?)\s*""#)?,
            possessive_echo: compile("possessive_echo", r"(\w) '(\w+) '\2\b")?,
            quote_echo: compile("quote_echo", r#"\b(\w+)" (\w)""#)?,
        })
    }

    /// Preserve document separator context for generic repairs while retaining
    /// one audit result per input paragraph, including rejected paragraphs.
    pub(super) fn clean(&self, paragraphs: &[&str]) -> Result<Vec<CleanedText>> {
        let mut results = Vec::with_capacity(paragraphs.len());
        for &text in paragraphs {
            anyhow::ensure!(
                !text.contains('\n'),
                "MuPDF cleanup requires reconstructed paragraphs without line breaks"
            );
            let mut result = CleanedText {
                text: text.to_owned(),
                dropped: false,
                repairs: Vec::new(),
            };
            if self.is_junk(text)? {
                record_repair(&mut result, "junk_paragraph", String::new());
                result.dropped = true;
            }
            results.push(result);
        }
        if results.iter().all(|result| result.text.is_empty()) {
            return Ok(results);
        }
        let mut document = filtered_document(&results);

        for (index, pass) in self.generic.iter().enumerate() {
            let next = substitute(&pass.regex, &document, pass.rule, pass.replacement)?;
            record_document_pass(&mut results, &mut document, next, pass.rule)?;
            if index == 0 {
                for (regex, rule, punctuation) in [
                    (&self.comma_echo, "comma_echo", ','),
                    (&self.period_echo, "period_echo", '.'),
                ] {
                    let next = remove_stutter(regex, &document, rule, punctuation)?;
                    record_document_pass(&mut results, &mut document, next, rule)?;
                }
            }
        }
        // The Python script resets double-quote pairing on every physical line;
        // an unmatched quote must never pair with one in another paragraph.
        let next = document
            .split('\n')
            .map(|line| substitute(&self.quote_pairs, line, "double_quote_pair", "\"${1}\""))
            .collect::<Result<Vec<_>>>()?
            .join("\n");
        record_document_pass(&mut results, &mut document, next, "double_quote_pair")?;
        let next = substitute(
            &self.possessive_echo,
            &document,
            "possessive_echo",
            "${1}'${2}",
        )?;
        record_document_pass(&mut results, &mut document, next, "possessive_echo")?;
        let next = replace_captures(&self.quote_echo, &document, "quote_echo", |captures| {
            let word = captured(captures, 1, "quote_echo")?;
            let echo = captured(captures, 2, "quote_echo")?;
            if echoes_final_character(word, echo) {
                Ok(format!("{word}\""))
            } else {
                Ok(captured(captures, 0, "quote_echo")?.to_owned())
            }
        })?;
        record_document_pass(&mut results, &mut document, next, "quote_echo")?;
        Ok(results)
    }

    /// Match the script's deliberately aggressive word ratio after layout
    /// joining; structural words and Markdown heading markers bypass removal.
    fn is_junk(&self, text: &str) -> Result<bool> {
        if text.trim().is_empty()
            || text.starts_with('#')
            || self
                .structural
                .is_match(text)
                .context("MuPDF cleanup rule junk_paragraph failed")?
        {
            return Ok(false);
        }
        let mut tokens = 0;
        let mut words = 0;
        for token in text.split_whitespace() {
            tokens += 1;
            if wordlike(token) {
                words += 1;
            }
        }
        Ok(words <= tokens / 2)
    }
}

/// Reproduce junk_line_pass on paragraph text followed by two newlines: a
/// rejected line disappears, blank lines remain, and newline runs cap at two.
/// Leading blank context matters to later lookbehind expressions.
fn filtered_document(results: &[CleanedText]) -> String {
    let mut lines = Vec::new();
    for result in results {
        if !result.dropped {
            lines.push(result.text.as_str());
        }
        lines.push("");
    }
    lines.push("");
    let uncollapsed = lines.join("\n");
    let mut document = String::with_capacity(uncollapsed.len());
    let mut newline_run = 0;
    for character in uncollapsed.chars() {
        if character == '\n' {
            newline_run += 1;
        } else {
            newline_run = 0;
        }
        if newline_run <= 2 {
            document.push(character);
        }
    }
    document
}

/// Split each changed document back onto its original surviving paragraphs.
/// Reject separator changes before recording repairs so text can never inherit
/// another paragraph's source locations through an unchecked positional zip.
fn record_document_pass(
    results: &mut [CleanedText],
    document: &mut String,
    next: String,
    rule: &'static str,
) -> Result<()> {
    if *document == next {
        return Ok(());
    }
    let leading = document.len() - document.trim_start_matches('\n').len();
    let trailing = document.len() - document.trim_end_matches('\n').len();
    let prefix = &document[..leading];
    let suffix = &document[document.len() - trailing..];
    let body = next
        .strip_prefix(prefix)
        .and_then(|text| text.strip_suffix(suffix))
        .with_context(|| {
            format!("MuPDF cleanup rule {rule} changed document boundary separators")
        })?;
    let paragraphs: Vec<&str> = body.split("\n\n").collect();
    let surviving = results
        .iter()
        .filter(|result| !result.dropped && !result.text.is_empty())
        .count();
    anyhow::ensure!(
        paragraphs.len() == surviving
            && paragraphs
                .iter()
                .all(|paragraph| !paragraph.is_empty() && !paragraph.contains('\n')),
        "MuPDF cleanup rule {rule} changed paragraph boundaries"
    );
    for (result, paragraph) in results
        .iter_mut()
        .filter(|result| !result.dropped && !result.text.is_empty())
        .zip(paragraphs)
    {
        record_repair(result, rule, paragraph.to_owned());
    }
    *document = next;
    Ok(())
}

/// Bound regex backtracking without dropping match errors or input text.
fn compile(rule: &'static str, pattern: &str, backtrack_limit: usize) -> Result<Regex> {
    RegexBuilder::new(pattern)
        .backtrack_limit(backtrack_limit)
        .build()
        .with_context(|| format!("failed to compile MuPDF cleanup rule {rule}"))
}

/// Accept exactly the script's ASCII-letter/apostrophe/hyphen core with at
/// least three letters; non-ASCII prose follows the same aggressive policy.
fn wordlike(token: &str) -> bool {
    let core = token.trim_matches(|character| TOKEN_EDGE_PUNCTUATION.contains(character));
    core.chars()
        .all(|character| character.is_ascii_alphabetic() || matches!(character, '\'' | '’' | '-'))
        && core
            .chars()
            .filter(char::is_ascii_alphabetic)
            .take(3)
            .count()
            == 3
}

/// Retain complete intermediate states only for rules that actually changed
/// text; the final state remains independently owned for canonical mapping.
fn record_repair(result: &mut CleanedText, rule: &'static str, after: String) {
    if result.text != after {
        let before = std::mem::replace(&mut result.text, after);
        result.repairs.push(Repair {
            rule,
            before,
            after: result.text.clone(),
        });
    }
}

/// Expand replacement captures through fallible iteration: replace_all would
/// hide runtime regex errors in an otherwise successful cleanup result.
fn substitute(regex: &Regex, text: &str, rule: &'static str, replacement: &str) -> Result<String> {
    replace_captures(regex, text, rule, |captures| {
        let mut replacement_text = String::new();
        captures.expand(replacement, &mut replacement_text);
        Ok(replacement_text)
    })
}

/// Preserve unmatched text exactly and attach the rule name to every matcher
/// failure, including errors occurring after earlier successful matches.
fn replace_captures(
    regex: &Regex,
    text: &str,
    rule: &'static str,
    mut replacement: impl FnMut(&Captures<'_>) -> Result<String>,
) -> Result<String> {
    let mut output = String::with_capacity(text.len());
    let mut position = 0;
    for captures in regex.captures_iter(text) {
        let captures = captures.with_context(|| format!("MuPDF cleanup rule {rule} failed"))?;
        let matched = captures
            .get(0)
            .with_context(|| format!("MuPDF cleanup rule {rule} omitted its full match"))?;
        output.push_str(&text[position..matched.start()]);
        output.push_str(&replacement(&captures)?);
        position = matched.end();
    }
    output.push_str(&text[position..]);
    Ok(output)
}

/// Treat missing captures as an internal rule error instead of panicking or
/// silently substituting empty strings into source content.
fn captured<'a>(captures: &Captures<'a>, index: usize, rule: &'static str) -> Result<&'a str> {
    captures
        .get(index)
        .map(|value| value.as_str())
        .with_context(|| format!("MuPDF cleanup rule {rule} omitted capture {index}"))
}

/// Compare letters case-insensitively without restricting names to ASCII.
fn echoes_final_character(word: &str, echo: &str) -> bool {
    word.chars()
        .last()
        .zip(echo.chars().next())
        .is_some_and(|(last, repeated)| last.to_lowercase().eq(repeated.to_lowercase()))
}

/// Retry overlapping candidates after a mismatch, as in the Python scan.
/// Every successful pass strictly shortens text, bounding the repeat loop;
/// single-letter words never collapse because real doubled initials are valid.
fn remove_stutter(
    regex: &Regex,
    text: &str,
    rule: &'static str,
    punctuation: char,
) -> Result<String> {
    let mut current = text.to_owned();
    loop {
        let mut changed = false;
        let mut position = 0;
        let mut output = String::with_capacity(current.len());
        while let Some(captures) = regex
            .captures_from_pos(&current, position)
            .with_context(|| format!("MuPDF cleanup rule {rule} failed"))?
        {
            let matched = captures
                .get(0)
                .with_context(|| format!("MuPDF cleanup rule {rule} omitted its full match"))?;
            let word = captured(&captures, 1, rule)?;
            let echo = captured(&captures, 2, rule)?;
            if word.chars().nth(1).is_some() && echoes_final_character(word, echo) {
                output.push_str(&current[position..matched.start()]);
                output.push_str(word);
                output.push(punctuation);
                position = matched.end();
                changed = true;
            } else {
                // Python advances one character after a rejected candidate;
                // Rust offsets are bytes, so advance across the whole UTF-8 scalar.
                let first = matched
                    .as_str()
                    .chars()
                    .next()
                    .with_context(|| format!("MuPDF cleanup rule {rule} matched empty text"))?;
                let next = matched.start() + first.len_utf8();
                output.push_str(&current[position..next]);
                position = next;
            }
        }
        output.push_str(&current[position..]);
        if !changed {
            return Ok(output);
        }
        current = output;
    }
}
