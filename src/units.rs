use std::path::Path;

use tokenizers::Tokenizer;

use crate::{config::RetrievalConfig, docling::DoclingConversionResult, error::ApiError};

#[derive(Debug, Clone)]
pub struct RetrievalUnit {
    pub unit_id: String,
    pub document_id: String,
    pub sequence: u32,
    pub source_path: String,
    pub heading_path: Vec<String>,
    pub page_numbers: Vec<u32>,
    pub content: String,
    pub token_count: usize,
}

#[derive(Debug, Clone)]
struct MarkdownBlock {
    content: String,
    heading_path: Vec<String>,
    page_numbers: Vec<u32>,
}

#[derive(Debug, Clone)]
struct UnitBuilder {
    content: String,
    heading_path: Vec<String>,
    page_numbers: Vec<u32>,
    token_count: usize,
}

/// Split one converted markdown document into deterministic retrieval units.
pub fn split_conversion_into_units(
    conversion: &DoclingConversionResult,
    retrieval: &RetrievalConfig,
    colbert_tokenizer_path: &Path,
) -> Result<Vec<RetrievalUnit>, ApiError> {
    let tokenizer =
        Tokenizer::from_file(colbert_tokenizer_path).map_err(|source| ApiError::UnitSplitting {
            message: format!(
                "failed to load ColBERT tokenizer at {}: {source}",
                colbert_tokenizer_path.display()
            ),
        })?;
    let blocks = parse_markdown_blocks(&conversion.markdown);
    let document_id = build_document_id(&conversion.source.relative_path);
    let source_path = conversion.source.relative_path.display().to_string();
    let mut units = Vec::new();
    let mut builder: Option<UnitBuilder> = None;
    let max_tokens = retrieval.max_unit_tokens as usize;
    let min_chars = retrieval.min_search_unit_chars as usize;

    for block in blocks {
        let token_count = count_tokens(&tokenizer, &block.content)?;
        if token_count > max_tokens {
            flush_unit(
                &mut builder,
                &mut units,
                &document_id,
                &source_path,
                min_chars,
            );
            split_oversized_block(
                &tokenizer,
                &block,
                max_tokens,
                min_chars,
                &document_id,
                &source_path,
                &mut units,
            )?;
            continue;
        }

        match builder.take() {
            Some(mut current) if can_append_block(&tokenizer, &current, &block, max_tokens)? => {
                let candidate = format!("{}\n\n{}", current.content, block.content);
                current.token_count = count_tokens(&tokenizer, &candidate)?;
                current.content.push_str("\n\n");
                current.content.push_str(&block.content);
                merge_page_numbers(&mut current.page_numbers, &block.page_numbers);
                builder = Some(current);
            }
            Some(current) => {
                let current_heading_path = current.heading_path.clone();
                let current_page_numbers = current.page_numbers.clone();
                let current_content = current.content;
                let current_token_count = current.token_count;
                push_unit(
                    &mut units,
                    &document_id,
                    &source_path,
                    current_heading_path,
                    current_page_numbers,
                    current_content,
                    current_token_count,
                    min_chars,
                );
                builder = Some(UnitBuilder::from_block(block, token_count));
            }
            None => {
                builder = Some(UnitBuilder::from_block(block, token_count));
            }
        }
    }

    flush_unit(
        &mut builder,
        &mut units,
        &document_id,
        &source_path,
        min_chars,
    );
    Ok(units)
}

impl UnitBuilder {
    /// Start a retrieval-unit builder from one parsed markdown block.
    fn from_block(block: MarkdownBlock, token_count: usize) -> Self {
        Self {
            content: block.content,
            heading_path: block.heading_path,
            page_numbers: block.page_numbers,
            token_count,
        }
    }
}

/// Parse normalized Docling markdown into heading-scoped paragraph blocks.
fn parse_markdown_blocks(markdown: &str) -> Vec<MarkdownBlock> {
    let mut blocks = Vec::new();
    let mut heading_stack: Vec<String> = Vec::new();
    let mut page_number: Option<u32> = None;
    let mut paragraph_lines: Vec<String> = Vec::new();

    for line in markdown.lines() {
        let trimmed = line.trim();
        if let Some(next_page_number) = parse_page_marker(trimmed) {
            flush_paragraph(
                &mut blocks,
                &mut paragraph_lines,
                &heading_stack,
                page_number,
            );
            page_number = Some(next_page_number);
            continue;
        }

        if let Some((level, heading)) = parse_heading(trimmed) {
            flush_paragraph(
                &mut blocks,
                &mut paragraph_lines,
                &heading_stack,
                page_number,
            );
            update_heading_stack(&mut heading_stack, level, heading);
            continue;
        }

        if trimmed.is_empty() {
            flush_paragraph(
                &mut blocks,
                &mut paragraph_lines,
                &heading_stack,
                page_number,
            );
            continue;
        }

        paragraph_lines.push(trimmed.to_string());
    }

    flush_paragraph(
        &mut blocks,
        &mut paragraph_lines,
        &heading_stack,
        page_number,
    );
    blocks
}

/// Move accumulated paragraph lines into a markdown block with current metadata.
fn flush_paragraph(
    blocks: &mut Vec<MarkdownBlock>,
    paragraph_lines: &mut Vec<String>,
    heading_stack: &[String],
    page_number: Option<u32>,
) {
    if paragraph_lines.is_empty() {
        return;
    }

    let content = paragraph_lines.join("\n");
    paragraph_lines.clear();
    if content.trim().is_empty() {
        return;
    }

    blocks.push(MarkdownBlock {
        content,
        heading_path: heading_stack.to_vec(),
        page_numbers: page_number.into_iter().collect(),
    });
}

/// Parse ATX markdown headings and return their level and text.
fn parse_heading(line: &str) -> Option<(usize, String)> {
    let hashes = line.chars().take_while(|value| *value == '#').count();
    if hashes == 0 || hashes > 6 {
        return None;
    }
    if !line.chars().nth(hashes).is_some_and(char::is_whitespace) {
        return None;
    }

    let heading = line[hashes..].trim().trim_matches('#').trim();
    if heading.is_empty() {
        return None;
    }

    Some((hashes, heading.to_string()))
}

/// Parse explicit page markers when Docling includes them in markdown output.
fn parse_page_marker(line: &str) -> Option<u32> {
    let normalized = line
        .trim_matches(|value: char| {
            matches!(
                value,
                '<' | '!' | '-' | '>' | '[' | ']' | '(' | ')' | '{' | '}' | '#' | ':'
            )
        })
        .trim();
    let lowered = normalized.to_ascii_lowercase();
    let page_prefix = lowered
        .strip_prefix("page ")
        .or_else(|| lowered.strip_prefix("page: "))
        .or_else(|| lowered.strip_prefix("page_number "))
        .or_else(|| lowered.strip_prefix("page_number: "))?;
    page_prefix
        .split_whitespace()
        .next()
        .and_then(|value| value.parse::<u32>().ok())
}

/// Update the heading stack while preserving markdown heading hierarchy.
fn update_heading_stack(heading_stack: &mut Vec<String>, level: usize, heading: String) {
    let target_len = level.saturating_sub(1);
    if heading_stack.len() > target_len {
        heading_stack.truncate(target_len);
    }
    while heading_stack.len() < target_len {
        heading_stack.push(String::new());
    }
    heading_stack.push(heading);
}

/// Return whether appending a block keeps the exact candidate text within the token cap.
fn can_append_block(
    tokenizer: &Tokenizer,
    current: &UnitBuilder,
    block: &MarkdownBlock,
    max_tokens: usize,
) -> Result<bool, ApiError> {
    if current.heading_path != block.heading_path {
        return Ok(false);
    }

    let candidate = format!("{}\n\n{}", current.content, block.content);
    Ok(count_tokens(tokenizer, &candidate)? <= max_tokens)
}

/// Split one block that exceeds the configured unit token cap.
fn split_oversized_block(
    tokenizer: &Tokenizer,
    block: &MarkdownBlock,
    max_tokens: usize,
    min_chars: usize,
    document_id: &str,
    source_path: &str,
    units: &mut Vec<RetrievalUnit>,
) -> Result<(), ApiError> {
    let mut builder: Option<UnitBuilder> = None;
    for sentence in split_sentences(&block.content) {
        let token_count = count_tokens(tokenizer, sentence)?;
        if token_count > max_tokens {
            flush_unit(&mut builder, units, document_id, source_path, min_chars);
            split_long_text_by_words(
                tokenizer,
                sentence,
                &block.heading_path,
                &block.page_numbers,
                max_tokens,
                min_chars,
                document_id,
                source_path,
                units,
            )?;
            continue;
        }

        let sentence_block = MarkdownBlock {
            content: sentence.to_string(),
            heading_path: block.heading_path.clone(),
            page_numbers: block.page_numbers.clone(),
        };

        match builder.take() {
            Some(mut current)
                if can_append_sentence(&tokenizer, &current, &sentence_block, max_tokens)? =>
            {
                let candidate = format!("{} {}", current.content, sentence_block.content);
                current.token_count = count_tokens(tokenizer, &candidate)?;
                current.content.push(' ');
                current.content.push_str(&sentence_block.content);
                merge_page_numbers(&mut current.page_numbers, &sentence_block.page_numbers);
                builder = Some(current);
            }
            Some(current) => {
                push_unit(
                    units,
                    document_id,
                    source_path,
                    current.heading_path,
                    current.page_numbers,
                    current.content,
                    current.token_count,
                    min_chars,
                );
                builder = Some(UnitBuilder::from_block(sentence_block, token_count));
            }
            None => {
                builder = Some(UnitBuilder::from_block(sentence_block, token_count));
            }
        }
    }

    flush_unit(&mut builder, units, document_id, source_path, min_chars);
    Ok(())
}

/// Return whether appending a sentence keeps the exact candidate text within the token cap.
fn can_append_sentence(
    tokenizer: &Tokenizer,
    current: &UnitBuilder,
    block: &MarkdownBlock,
    max_tokens: usize,
) -> Result<bool, ApiError> {
    if current.heading_path != block.heading_path {
        return Ok(false);
    }

    let candidate = format!("{} {}", current.content, block.content);
    Ok(count_tokens(tokenizer, &candidate)? <= max_tokens)
}

/// Split a very long sentence or table row by words when sentence splitting is insufficient.
fn split_long_text_by_words(
    tokenizer: &Tokenizer,
    text: &str,
    heading_path: &[String],
    page_numbers: &[u32],
    max_tokens: usize,
    min_chars: usize,
    document_id: &str,
    source_path: &str,
    units: &mut Vec<RetrievalUnit>,
) -> Result<(), ApiError> {
    let mut current_words = Vec::new();

    for word in text.split_whitespace() {
        let candidate = build_word_candidate(&current_words, word);
        let candidate_tokens = count_tokens(tokenizer, &candidate)?;
        if !current_words.is_empty() && candidate_tokens > max_tokens {
            let content = current_words.join(" ");
            let token_count = count_tokens(tokenizer, &content)?;
            push_unit(
                units,
                document_id,
                source_path,
                heading_path.to_vec(),
                page_numbers.to_vec(),
                content,
                token_count,
                min_chars,
            );
            current_words.clear();
        }

        current_words.push(word.to_string());
    }

    if !current_words.is_empty() {
        let content = current_words.join(" ");
        let token_count = count_tokens(tokenizer, &content)?;
        push_unit(
            units,
            document_id,
            source_path,
            heading_path.to_vec(),
            page_numbers.to_vec(),
            content,
            token_count,
            min_chars,
        );
    }

    Ok(())
}

/// Build the candidate text for adding one word to the current word chunk.
fn build_word_candidate(current_words: &[String], word: &str) -> String {
    if current_words.is_empty() {
        return word.to_string();
    }

    format!("{} {}", current_words.join(" "), word)
}

/// Flush the current builder into the unit accumulator.
fn flush_unit(
    builder: &mut Option<UnitBuilder>,
    units: &mut Vec<RetrievalUnit>,
    document_id: &str,
    source_path: &str,
    min_chars: usize,
) {
    if let Some(current) = builder.take() {
        push_unit(
            units,
            document_id,
            source_path,
            current.heading_path,
            current.page_numbers,
            current.content,
            current.token_count,
            min_chars,
        );
    }
}

/// Add one retrieval unit when it has enough searchable content.
fn push_unit(
    units: &mut Vec<RetrievalUnit>,
    document_id: &str,
    source_path: &str,
    heading_path: Vec<String>,
    page_numbers: Vec<u32>,
    content: String,
    token_count: usize,
    min_chars: usize,
) {
    let normalized_content = normalize_unit_content(&content);
    if normalized_content.chars().count() < min_chars {
        return;
    }

    let sequence = units.len() as u32;
    units.push(RetrievalUnit {
        unit_id: format!("{document_id}:unit:{sequence:06}"),
        document_id: document_id.to_string(),
        sequence,
        source_path: source_path.to_string(),
        heading_path: heading_path
            .into_iter()
            .filter(|value| !value.trim().is_empty())
            .collect(),
        page_numbers: unique_page_numbers(page_numbers),
        content: normalized_content,
        token_count,
    });
}

/// Split paragraph content into sentence-like chunks without adding a parser dependency.
fn split_sentences(text: &str) -> Vec<&str> {
    let mut results = Vec::new();
    let mut start = 0;
    let mut previous_end = 0;

    for (index, value) in text.char_indices() {
        let end = index + value.len_utf8();
        previous_end = end;
        if !matches!(value, '.' | '?' | '!' | '\n') {
            continue;
        }

        let candidate = text[start..end].trim();
        if !candidate.is_empty() {
            results.push(candidate);
        }
        start = end;
    }

    if start < previous_end {
        let candidate = text[start..].trim();
        if !candidate.is_empty() {
            results.push(candidate);
        }
    }
    if results.is_empty() && !text.trim().is_empty() {
        results.push(text.trim());
    }

    results
}

/// Count model tokens for one unit candidate.
fn count_tokens(tokenizer: &Tokenizer, text: &str) -> Result<usize, ApiError> {
    tokenizer
        .encode(text, true)
        .map(|encoding| encoding.len())
        .map_err(|source| ApiError::UnitSplitting {
            message: format!("ColBERT tokenization failed during unit splitting: {source}"),
        })
}

/// Merge page metadata while preserving numeric ordering.
fn merge_page_numbers(target: &mut Vec<u32>, source: &[u32]) {
    target.extend(source.iter().copied());
    *target = unique_page_numbers(std::mem::take(target));
}

/// Sort and deduplicate page numbers for deterministic unit metadata.
fn unique_page_numbers(mut page_numbers: Vec<u32>) -> Vec<u32> {
    page_numbers.sort_unstable();
    page_numbers.dedup();
    page_numbers
}

/// Normalize unit content after chunk assembly.
fn normalize_unit_content(content: &str) -> String {
    content
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string()
}

/// Build a deterministic document identifier from the corpus-relative source path.
pub(crate) fn build_document_id(path: &Path) -> String {
    let source = path.display().to_string();
    let mut slug = String::with_capacity(source.len());
    for value in source.chars() {
        if value.is_ascii_alphanumeric() {
            slug.push(value.to_ascii_lowercase());
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }

    slug.trim_matches('-').to_string()
}
