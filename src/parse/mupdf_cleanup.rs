//! Script-based PDF text preparation shared by production and the offline preview.
//! Native extraction remains untouched; every surviving paragraph carries its
//! original line references, and the report records discarded text and ordered repairs.

// The diagnostic includes this module by path; pin the child path so both
// crate layouts compile the same rule implementation.
#[path = "mupdf_cleanup/text.rs"]
mod text;

use anyhow::{Context, Result};
use fancy_regex::Regex;
use serde::Serialize;

use super::native_pdf::{BlockKind, ExtractedPdf};
use text::{Repair, TextCleaner};

/// Bump for any change to preparation or repair rules; this enters parser identity.
pub(crate) const CLEANUP_VERSION: &str = "1";
/// Shared artifact name used by production bundles and the standalone preview.
pub(crate) const REPORT_FILE_NAME: &str = "mupdf_cleanup.json";
// These are the reference extractor's margin bands, in page-relative PDF points.
const TOP_BAND_POINTS: f32 = 50.0;
const BOTTOM_BAND_POINTS: f32 = 25.0;
const TERMINAL: &str = ".!?:;\"'’”)]";

/// A source line in the archived native JSON, before trimming, joining, or removal.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SourceLine {
    pub(crate) page_number: u64,
    pub(crate) block_index: usize,
    pub(crate) line_index: usize,
    pub(crate) bounds: [f32; 4],
}

/// Ready-to-map paragraph; all source lines survive even when a paragraph spans pages.
pub(crate) struct CleanedParagraph {
    pub(crate) text: String,
    pub(crate) sources: Vec<SourceLine>,
}

/// Text dropped before paragraph construction; source references point into the raw archive.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RemovedText {
    reason: &'static str,
    sources: Vec<SourceLine>,
    text: String,
}

/// Prepared text and each subsequent repair preserve the full transformation sequence.
/// A missing cleaned_text means the aggressive junk filter discarded this paragraph.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ParagraphTrace {
    sources: Vec<SourceLine>,
    prepared_text: String,
    cleaned_text: Option<String>,
    repairs: Vec<Repair>,
}

/// Durable cleanup evidence belongs in parser artifacts, never in the service log.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CleanupReport {
    version: &'static str,
    top_band_points: f32,
    bottom_band_points: f32,
    pub(crate) input_text_blocks: usize,
    pub(crate) joined_blocks: usize,
    pub(crate) removed_lines: Vec<RemovedText>,
    pub(crate) dropped_paragraphs: usize,
    pub(crate) repair_passes: usize,
    pub(crate) output_paragraphs: usize,
    paragraphs: Vec<ParagraphTrace>,
}

/// Runtime paragraphs and their audit report are produced by the same cleaning pass.
pub(crate) struct CleanedDocument {
    pub(crate) paragraphs: Vec<CleanedParagraph>,
    pub(crate) report: CleanupReport,
}

/// Apply the supplied scripts' preparation and generic cleanup, without font rules,
/// hierarchy inference, OCR, or book-specific replacements.
pub(crate) fn clean_document(
    document: &ExtractedPdf,
    regex_backtrack_limit: usize,
) -> Result<CleanedDocument> {
    let cleaner = TextCleaner::new(regex_backtrack_limit)?;
    let folio = Regex::new(r"^(?:\d+|[ivxlcdm]+)$").context("compile PDF folio matcher")?;
    let mut report = CleanupReport {
        version: CLEANUP_VERSION,
        top_band_points: TOP_BAND_POINTS,
        bottom_band_points: BOTTOM_BAND_POINTS,
        input_text_blocks: 0,
        joined_blocks: 0,
        removed_lines: Vec::new(),
        dropped_paragraphs: 0,
        repair_passes: 0,
        output_paragraphs: 0,
        paragraphs: Vec::new(),
    };
    let mut prepared: Vec<CleanedParagraph> = Vec::new();
    for page in &document.pages {
        for (block_index, block) in page.blocks.iter().enumerate() {
            if !matches!(block.kind, BlockKind::Text) {
                continue;
            }
            report.input_text_blocks += 1;
            let mut lines = Vec::new();
            let mut sources = Vec::new();
            for (line_index, line) in block.lines.iter().enumerate() {
                let source = SourceLine {
                    page_number: page.page_number,
                    block_index,
                    line_index,
                    bounds: line.bounds,
                };
                let raw_text = line
                    .spans
                    .iter()
                    .map(|span| span.text.as_str())
                    .collect::<String>();
                let line_text = line
                    .spans
                    .iter()
                    .map(|span| span.text.trim())
                    .filter(|span| !span.is_empty())
                    .collect::<Vec<_>>()
                    .join(" ");
                let reason = if line.bounds[3] < page.bounds[1] + TOP_BAND_POINTS {
                    Some("top_margin")
                } else if line.bounds[1] > page.bounds[3] - BOTTOM_BAND_POINTS {
                    Some("bottom_margin")
                } else if line_text.is_empty() {
                    Some("empty_line")
                } else {
                    None
                };
                if let Some(reason) = reason {
                    report.removed_lines.push(RemovedText {
                        reason,
                        sources: vec![source],
                        text: raw_text,
                    });
                } else {
                    sources.push(source);
                    lines.push(line_text);
                }
            }
            let Some(first) = lines.first() else {
                continue;
            };
            let mut paragraph = first.clone();
            for next in &lines[1..] {
                join_line(&mut paragraph, next);
            }
            if lines.len() == 1 && folio.is_match(&paragraph).context("match PDF folio")? {
                report.removed_lines.push(RemovedText {
                    reason: "folio",
                    sources,
                    text: paragraph,
                });
                continue;
            }
            if let Some(previous) = prepared.last_mut()
                && continues_paragraph(&previous.text, &paragraph)
            {
                // Match the extractor's cross-block rule: a trailing hyphen joins
                // directly; otherwise an unfinished paragraph gains one space.
                if previous.text.ends_with('-') {
                    previous.text.pop();
                } else {
                    previous.text.push(' ');
                }
                previous.text.push_str(&paragraph);
                previous.sources.extend(sources);
                report.joined_blocks += 1;
            } else {
                prepared.push(CleanedParagraph {
                    text: paragraph,
                    sources,
                });
            }
        }
    }
    // Junk filtering uses complete paragraphs; subsequent regex passes retain
    // the script's document separators and cross-paragraph quote context.
    let texts = prepared
        .iter()
        .map(|paragraph| paragraph.text.as_str())
        .collect::<Vec<_>>();
    let cleaned_paragraphs = cleaner
        .clean(&texts)
        .context("clean reconstructed PDF text")?;
    anyhow::ensure!(
        cleaned_paragraphs.len() == prepared.len(),
        "PDF cleanup changed paragraph source accounting"
    );
    let mut paragraphs = Vec::new();
    for (paragraph, cleaned) in prepared.into_iter().zip(cleaned_paragraphs) {
        report.repair_passes += cleaned.repairs.len();
        report.dropped_paragraphs += usize::from(cleaned.dropped);
        let cleaned_text = (!cleaned.dropped).then_some(cleaned.text);
        if let Some(text) = &cleaned_text {
            // Runtime mapping and audit serialization need independent owned
            // copies; both originate here, after the same ordered cleanup pass.
            paragraphs.push(CleanedParagraph {
                text: text.clone(),
                sources: paragraph.sources.clone(),
            });
        }
        report.paragraphs.push(ParagraphTrace {
            sources: paragraph.sources,
            prepared_text: paragraph.text,
            cleaned_text,
            repairs: cleaned.repairs,
        });
    }
    report.output_paragraphs = paragraphs.len();
    Ok(CleanedDocument { paragraphs, report })
}

/// Heal line-wrap hyphens before lowercase continuations; otherwise join with one space.
fn join_line(previous: &mut String, next: &str) {
    if previous.ends_with('-') && next.chars().next().is_some_and(char::is_lowercase) {
        previous.pop();
    } else {
        previous.push(' ');
    }
    previous.push_str(next);
}

/// Mirror the reference extractor's punctuation rule across native blocks and pages.
fn continues_paragraph(previous: &str, next: &str) -> bool {
    previous.ends_with('-')
        || (previous
            .chars()
            .last()
            .is_some_and(|c| !TERMINAL.contains(c))
            && next.chars().next().is_some_and(char::is_lowercase))
}
