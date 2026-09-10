//! Lossless, bounded source excerpts for annotation requests.

use crate::error::ApiError;

/// One contiguous source fragment with Unicode scalar offsets and an exclusive end.
pub(crate) struct TextExcerpt {
    pub start_char: usize,
    pub end_char: usize,
    pub text: String,
}

/// Partition source text without trimming or dropping oversized-unit tails.
pub(crate) fn split_text(text: &str, max_chars: usize) -> Result<Vec<TextExcerpt>, ApiError> {
    if max_chars == 0 {
        return Err(ApiError::AnnotationProducer {
            message: "cannot split annotation input: max_input_chars must be greater than zero"
                .to_string(),
        });
    }

    // Keep byte positions alongside scalar values so provenance offsets and UTF-8
    // slicing describe exactly the same partition, including all whitespace.
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let mut excerpts = Vec::new();
    let mut start = 0;
    while start < chars.len() {
        let limit = start.saturating_add(max_chars).min(chars.len());
        let end = if limit == chars.len() {
            limit
        } else {
            preferred_end(&chars, start, limit)
        };
        let end_byte = chars.get(end).map_or(text.len(), |(byte, _)| *byte);
        excerpts.push(TextExcerpt {
            start_char: start,
            end_char: end,
            text: text[chars[start].0..end_byte].to_string(),
        });
        start = end;
    }
    Ok(excerpts)
}

/// Prefer coherent boundaries while always advancing within the scalar budget.
fn preferred_end(chars: &[(usize, char)], start: usize, limit: usize) -> usize {
    let mut paragraph_end = None;
    let mut sentence_end = None;
    let mut whitespace_end = None;
    let mut line_breaks = 0;
    let mut sentence_terminal = false;
    let mut has_text = false;

    for index in start..limit {
        let ch = chars[index].1;
        if ch.is_whitespace() {
            whitespace_end = Some(index + 1);
            // Count CRLF as one line break; blank lines may contain indentation.
            if ch == '\n'
                || (ch == '\r' && chars.get(index + 1).is_none_or(|(_, next)| *next != '\n'))
            {
                line_breaks += 1;
            }
            if has_text && line_breaks >= 2 {
                paragraph_end = Some(index + 1);
            }
            if sentence_terminal {
                sentence_end = Some(index + 1);
            }
        } else {
            has_text = true;
            line_breaks = 0;
            // This is a boundary heuristic, not linguistic parsing. Closing
            // quotation marks/brackets retain preceding sentence punctuation.
            if !matches!(ch, '\'' | '"' | '’' | '”' | ')' | ']' | '}') {
                sentence_terminal = matches!(ch, '.' | '!' | '?' | '。' | '！' | '？');
            }
        }
    }

    // A complete word or sentence can end exactly at the cap, with its separator
    // retained at the beginning of the next fragment instead of wasting capacity.
    if chars
        .get(limit)
        .is_some_and(|(_, next)| next.is_whitespace())
    {
        whitespace_end = Some(limit);
        if sentence_terminal {
            sentence_end = Some(limit);
        }
    }

    paragraph_end
        .or(sentence_end)
        .or(whitespace_end)
        .unwrap_or(limit)
}
