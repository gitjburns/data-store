//! Semantic section kind table (SPEC-epub §7.2), versioned by
//! `KIND_PATTERNS_VERSION` for parser identity (§3.2). The kind enum itself
//! is `crate::model::body::SectionKind`; this module holds only the mapping
//! table. Structure only (§1.3): no item here reads label or heading text.

use crate::model::body::SectionKind;

/// Version of the kind table folded into `parserConfigHash`. Bump on any
/// change to the table.
pub(crate) const KIND_PATTERNS_VERSION: &str = "1";

/// SPEC-epub §7.2 kind table for `epub:type`, `data-type`, landmark, and
/// guide values (rules 1 to 3). Values are matched exactly, per
/// whitespace-separated token. Values absent here (`bodymatter`,
/// `frontmatter`, `backmatter`, `book`, ...) deliberately do not match so the
/// next rule applies.
const KIND_TABLE: &[(&str, SectionKind)] = &[
    ("cover", SectionKind::Cover),
    ("titlepage", SectionKind::Titlepage),
    ("halftitlepage", SectionKind::Titlepage),
    ("title-page", SectionKind::Titlepage),
    ("copyright-page", SectionKind::CopyrightPage),
    ("toc", SectionKind::Toc),
    ("preface", SectionKind::Preface),
    ("foreword", SectionKind::Foreword),
    ("introduction", SectionKind::Introduction),
    ("prologue", SectionKind::Prologue),
    ("epilogue", SectionKind::Epilogue),
    ("afterword", SectionKind::Afterword),
    ("conclusion", SectionKind::Conclusion),
    ("part", SectionKind::Part),
    ("volume", SectionKind::Part),
    ("chapter", SectionKind::Chapter),
    ("subchapter", SectionKind::Section),
    ("division", SectionKind::Section),
    ("sect1", SectionKind::Section),
    ("sect2", SectionKind::Section),
    ("sect3", SectionKind::Section),
    ("sect4", SectionKind::Section),
    ("sect5", SectionKind::Section),
    ("appendix", SectionKind::Appendix),
    ("glossary", SectionKind::Glossary),
    ("bibliography", SectionKind::Bibliography),
    ("index", SectionKind::Index),
    ("acknowledgments", SectionKind::Acknowledgments),
    ("dedication", SectionKind::Dedication),
    ("epigraph", SectionKind::Epigraph),
    ("colophon", SectionKind::Colophon),
    ("endnotes", SectionKind::Notes),
    ("footnotes", SectionKind::Notes),
    ("rearnotes", SectionKind::Notes),
    ("notes", SectionKind::Notes),
    // Guide type (EPUB 2 `reference type="text"`), the main body start.
    ("text", SectionKind::Chapter),
];

/// Kind table lookup for `epub:type`, `data-type`, landmark, and guide
/// values (§7.2 rules 1 to 3); a space-separated value matches on any token,
/// the first matching token in value order deciding.
pub(crate) fn kind_from_semantic(value: &str) -> Option<SectionKind> {
    value.split_whitespace().find_map(|token| {
        KIND_TABLE
            .iter()
            .find(|(name, _)| *name == token)
            .map(|(_, kind)| *kind)
    })
}
