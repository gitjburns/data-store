//! Native PDF extraction for the standalone evaluation diagnostic.
//! This module preserves MuPDF's block/line order and text without cleanup.

use std::panic::{UnwindSafe, catch_unwind};
use std::path::Path;

use anyhow::{Context, Result, anyhow, ensure};
use mupdf::text_page::TextBlockType;
use mupdf::{
    Document, MetadataName, Quad, Rect, TextBlock, TextCharFlags, TextLine, TextPageFlags,
};
use serde::Serialize;

/// Exact extraction options recorded beside diagnostic output; no OCR or dehyphenation.
pub(crate) const EXTRACTION_FLAGS: TextPageFlags =
    TextPageFlags::PRESERVE_IMAGES.union(TextPageFlags::COLLECT_STYLES);

/// Owned document output; pages without native text remain represented.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExtractedPdf {
    pub(crate) pages: Vec<ExtractedPage>,
}

/// Physical page position and native page geometry, with every extracted block.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExtractedPage {
    pub(crate) page_number: u64,
    pub(crate) bounds: [f32; 4],
    pub(crate) blocks: Vec<ExtractedBlock>,
}

/// Known native block categories are shared with diagnostic accounting so adding
/// a category requires an explicit decision in each consumer.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BlockKind {
    Text,
    Image,
    Struct,
    Vector,
    Grid,
}

/// Native block category and geometry; non-text blocks have no text lines.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExtractedBlock {
    pub(crate) kind: BlockKind,
    pub(crate) bounds: [f32; 4],
    pub(crate) lines: Vec<ExtractedLine>,
}

/// A native baseline group, retaining source order rather than inferred reading order.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExtractedLine {
    pub(crate) bounds: [f32; 4],
    pub(crate) spans: Vec<ExtractedSpan>,
}

/// Adjacent native characters sharing font, style, exact size, and character flags.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExtractedSpan {
    pub(crate) text: String,
    pub(crate) font_name: Option<String>,
    pub(crate) size: f32,
    pub(crate) bold: bool,
    pub(crate) italic: bool,
    pub(crate) monospaced: bool,
    pub(crate) serif: bool,
    /// Native fill color packed as 0xAARRGGBB in sRGB.
    pub(crate) argb: u32,
    pub(crate) bounds: [f32; 4],
    pub(crate) flags: u16,
}

/// Additional native style comparisons shared by grouping and exported span fields.
#[derive(Clone, Copy, PartialEq, Eq)]
struct CharacterAppearance {
    italic: bool,
    monospaced: bool,
    serif: bool,
    argb: u32,
}

/// Extract every physical page synchronously, failing with file/page context.
/// Native page and text-page handles are dropped each iteration; only owned data escapes.
pub(crate) fn extract_pdf(path: &Path) -> Result<ExtractedPdf> {
    let path_text = path
        .to_str()
        .with_context(|| format!("PDF path is not UTF-8: {}", path.display()))?;
    let document =
        Document::open(path_text).with_context(|| format!("opening PDF {}", path.display()))?;
    ensure!(document.is_pdf(), "input is not a PDF: {}", path.display());
    ensure!(
        !document
            .needs_password()
            .with_context(|| format!("checking PDF password requirement: {}", path.display()))?,
        "encrypted PDF requires a password: {}",
        path.display()
    );
    // MuPDF returns exactly "None" for PDFs without a crypt dictionary. Checking
    // metadata also rejects encrypted PDFs that open with an empty user password.
    let encryption = document
        .metadata(MetadataName::Encryption)
        .with_context(|| format!("reading PDF encryption metadata: {}", path.display()))?;
    ensure!(
        encryption == "None",
        "encrypted or indeterminate PDF is unsupported: {} (encryption: {encryption:?})",
        path.display()
    );
    let page_count = document
        .page_count()
        .with_context(|| format!("counting PDF pages: {}", path.display()))?;
    ensure!(
        page_count >= 0,
        "PDF has invalid page count {page_count}: {}",
        path.display()
    );
    let mut pages = Vec::new();
    for page_index in 0..page_count {
        let page_number = page_index as u64 + 1;
        let extracted = extract_page(&document, page_index, page_number)
            .with_context(|| format!("extracting PDF {} page {page_number}", path.display()))?;
        pages.push(extracted);
    }
    Ok(ExtractedPdf { pages })
}

/// Own native handles only for this page; image-only and empty pages are retained.
fn extract_page(document: &Document, page_index: i32, page_number: u64) -> Result<ExtractedPage> {
    let page = document
        .load_page(page_index)
        .context("loading native page")?;
    let bounds = checked_rect(page.bounds().context("reading page bounds")?)?;
    let text_page = page
        .to_text_page(EXTRACTION_FLAGS)
        .context("extracting native structured text")?;
    let mut blocks = Vec::new();
    for (index, block) in text_page.blocks().enumerate() {
        blocks.push(extract_block(&block).with_context(|| format!("native block {index}"))?);
    }
    Ok(ExtractedPage {
        page_number,
        bounds,
        blocks,
    })
}

/// Preserve the native block category instead of dropping non-text output.
fn extract_block(block: &TextBlock<'_>) -> Result<ExtractedBlock> {
    let block_type = checked_accessor(|| block.r#type(), "native block type")?;
    let kind = match block_type {
        TextBlockType::Text => BlockKind::Text,
        TextBlockType::Image => BlockKind::Image,
        TextBlockType::Struct => BlockKind::Struct,
        TextBlockType::Vector => BlockKind::Vector,
        TextBlockType::Grid => BlockKind::Grid,
    };
    let bounds = checked_rect(block.bounds()).context("block geometry")?;
    let mut lines = Vec::new();
    for (index, line) in block.lines().enumerate() {
        lines.push(extract_line(&line).with_context(|| format!("native line {index}"))?);
    }
    Ok(ExtractedBlock {
        kind,
        bounds,
        lines,
    })
}

/// Group only adjacent style-identical characters; never trim, insert, or normalize text.
fn extract_line(line: &TextLine<'_>) -> Result<ExtractedLine> {
    let bounds = checked_rect(line.bounds()).context("line geometry")?;
    let mut spans: Vec<ExtractedSpan> = Vec::new();
    let mut previous_appearance = None;
    for (index, character) in line.chars().enumerate() {
        let value = character
            .char()
            .with_context(|| format!("native character {index} is not a valid Unicode scalar"))?;
        let size = character.size();
        ensure!(
            size.is_finite(),
            "native character {index} has non-finite font size {size}"
        );
        let bounds = checked_quad(character.quad())
            .with_context(|| format!("native character {index} geometry"))?;
        let font = character.font();
        // The retained native font owns this borrowed name for the comparison;
        // allocate an owned name only when a distinct output span starts.
        let font_name = font
            .as_ref()
            .map(|font| checked_accessor(|| font.name(), "native font name"))
            .transpose()
            .with_context(|| format!("native character {index} font"))?;
        let flags = character.flags();
        let bold =
            flags.contains(TextCharFlags::BOLD) || font.as_ref().is_some_and(|font| font.is_bold());
        let appearance = CharacterAppearance {
            italic: font.as_ref().is_some_and(|font| font.is_italic()),
            monospaced: font.as_ref().is_some_and(|font| font.is_monospaced()),
            serif: font.as_ref().is_some_and(|font| font.is_serif()),
            argb: character.argb(),
        };
        if let Some(span) = spans.last_mut()
            && span.font_name.as_deref() == font_name
            && span.size.to_bits() == size.to_bits()
            && span.bold == bold
            && span.flags == flags.bits()
            && previous_appearance == Some(appearance)
        {
            span.text.push(value);
            span.bounds[0] = span.bounds[0].min(bounds[0]);
            span.bounds[1] = span.bounds[1].min(bounds[1]);
            span.bounds[2] = span.bounds[2].max(bounds[2]);
            span.bounds[3] = span.bounds[3].max(bounds[3]);
        } else {
            spans.push(ExtractedSpan {
                text: value.to_string(),
                font_name: font_name.map(str::to_owned),
                size,
                bold,
                italic: appearance.italic,
                monospaced: appearance.monospaced,
                serif: appearance.serif,
                argb: appearance.argb,
                bounds,
                flags: flags.bits(),
            });
        }
        previous_appearance = Some(appearance);
    }
    Ok(ExtractedLine { bounds, spans })
}

/// Reject non-finite geometry before serde could silently encode it as null.
fn checked_rect(rect: Rect) -> Result<[f32; 4]> {
    let bounds = [rect.x0, rect.y0, rect.x1, rect.y1];
    ensure!(
        bounds.iter().all(|value| value.is_finite()),
        "non-finite rectangle {bounds:?}"
    );
    Ok(bounds)
}

/// Compute an axis-aligned character box from all four validated quad vertices.
fn checked_quad(quad: Quad) -> Result<[f32; 4]> {
    let points = [quad.ul, quad.ur, quad.ll, quad.lr];
    ensure!(
        points
            .iter()
            .all(|point| point.x.is_finite() && point.y.is_finite()),
        "non-finite character quadrilateral {points:?}"
    );
    let mut bounds = [points[0].x, points[0].y, points[0].x, points[0].y];
    for point in &points[1..] {
        bounds[0] = bounds[0].min(point.x);
        bounds[1] = bounds[1].min(point.y);
        bounds[2] = bounds[2].max(point.x);
        bounds[3] = bounds[3].max(point.y);
    }
    Ok(bounds)
}

/// Convert the binding's two infallible accessor panics into terminal errors.
/// MuPDF 0.8 unwraps unknown block tags and invalid UTF-8 font names internally;
/// these immutable reads are never retried and extraction never returns partial data.
fn checked_accessor<T>(read: impl FnOnce() -> T + UnwindSafe, field: &str) -> Result<T> {
    catch_unwind(read).map_err(|payload| {
        let detail = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic payload");
        anyhow!("MuPDF rejected malformed {field}: {detail}")
    })
}
