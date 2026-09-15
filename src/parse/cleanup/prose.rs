//! Conservative cleanup for parser-declared prose; uncertain formatting stays verbatim.

/// Repair extraction spacing without guessing missing words or changing
/// line structure.
pub(super) fn clean_prose(text: &str) -> String {
    if is_protected(text) {
        return text.to_string();
    }

    // Keep each original line ending, including CRLF and a final newline.
    let mut cleaned = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let (body, ending) = if let Some(body) = line.strip_suffix("\r\n") {
            (body, "\r\n")
        } else if let Some(body) = line.strip_suffix('\n') {
            (body, "\n")
        } else {
            (line, "")
        };
        cleaned.push_str(&repair_contractions(&normalize_horizontal(body)));
        cleaned.push_str(ending);
    }
    cleaned
}

/// Whitespace can carry syntax in code, mathematics, or indented material. A
/// paragraph label alone is insufficient evidence to rewrite those strings.
/// Isolated numbers and punctuation also remain untouched.
pub(super) fn is_protected(text: &str) -> bool {
    !text.chars().any(char::is_alphabetic)
        || text.chars().any(|character| {
            matches!(
                character,
                '`' | '\\'
                    | '|'
                    | '{'
                    | '}'
                    | '='
                    | '$'
                    | '^'
                    | '<'
                    | '>'
                    | '_'
                    | '['
                    | ']'
                    | '+'
                    | '*'
                    | '/'
                    | '%'
                    | '#'
                    | '×'
                    | '÷'
                    | '±'
            ) || is_unicode_math_notation(character)
                || (character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
        })
        || text.contains("::")
        || text.contains(" - ")
        || text
            .split(['\n', '\r', '\u{2028}', '\u{2029}'])
            .any(|line| {
                // Blank lines carry no indentation evidence. Content-bearing
                // lines still protect leading spaces and tabs anywhere in the line.
                !line.trim().is_empty()
                    && (line.contains('\t') || line.chars().next().is_some_and(is_horizontal_space))
            })
}

/// Unicode notation can encode equations without ASCII operators. Protect its
/// standard symbol blocks, including styled mathematical letters, while leaving
/// ordinary non-ASCII prose eligible for cleanup. Block-level detection also
/// conservatively protects ambiguous uses such as superscript footnote markers.
fn is_unicode_math_notation(character: char) -> bool {
    matches!(
        character,
        '\u{00b2}' | '\u{00b3}' | '\u{00b9}' | '\u{00bc}'..='\u{00be}'
            | '\u{2044}' // Fraction slash.
            | '\u{2070}'..='\u{209f}' // Superscripts and subscripts.
            | '\u{2100}'..='\u{214f}' // Letterlike symbols.
            | '\u{2150}'..='\u{218f}' // Number forms, including fractions.
            | '\u{2190}'..='\u{21ff}' // Arrows.
            | '\u{2200}'..='\u{22ff}' // Mathematical operators, including −, ∈, ∫.
            | '\u{2308}'..='\u{230b}' // Ceiling and floor brackets.
            | '\u{2320}'..='\u{2321}' // Integral halves.
            | '\u{239b}'..='\u{23b3}' // Extensible brackets and integrals.
            | '\u{23dc}'..='\u{23e1}' // Mathematical grouping marks.
            | '\u{27c0}'..='\u{27ff}' // Miscellaneous math A and supplemental arrows A.
            | '\u{2900}'..='\u{29ff}' // Supplemental arrows B and miscellaneous math B.
            | '\u{2a00}'..='\u{2aff}' // Supplemental mathematical operators.
            | '\u{2b00}'..='\u{2bff}' // Miscellaneous symbols and arrows.
            | '\u{1d400}'..='\u{1d7ff}' // Mathematical alphanumeric symbols.
            | '\u{1ee00}'..='\u{1eeff}' // Arabic mathematical alphabetic symbols.
            | '\u{1f800}'..='\u{1f8ff}' // Supplemental arrows C.
    )
}

/// Normalize extraction whitespace only within a line. A soft hyphen between
/// letters is discretionary; every hard hyphen and all other characters remain.
fn normalize_horizontal(text: &str) -> String {
    let characters: Vec<char> = text.chars().collect();
    let mut cleaned = String::with_capacity(text.len());
    let mut pending_space = false;
    for (index, &character) in characters.iter().enumerate() {
        if is_horizontal_space(character) {
            pending_space = true;
            continue;
        }
        if character == '\u{00ad}'
            && index > 0
            && characters[index - 1].is_alphabetic()
            && characters
                .get(index + 1)
                .is_some_and(|next| next.is_alphabetic())
        {
            continue;
        }
        if pending_space
            && !cleaned.is_empty()
            && !matches!(character, ',' | '.' | ';' | ':' | '!' | '?')
        {
            cleaned.push(' ');
        }
        cleaned.push(character);
        pending_space = false;
    }
    cleaned
}

/// Restrict space folding to horizontal Unicode spacing; line separators and
/// arbitrary control characters are not interchangeable with ordinary spaces.
fn is_horizontal_space(character: char) -> bool {
    matches!(
        character,
        ' ' | '\u{00a0}' | '\u{2000}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}'
    )
}

/// Remove only spaces around a recognized contraction apostrophe. Adjacent
/// quotation marks veto repair so quoted letters or words are not consumed.
fn repair_contractions(text: &str) -> String {
    let characters: Vec<char> = text.chars().collect();
    let mut omit = vec![false; characters.len()];
    for (index, &character) in characters.iter().enumerate() {
        if !matches!(character, '\'' | '’') {
            continue;
        }
        let mut left_end = index;
        while left_end > 0 && characters[left_end - 1] == ' ' {
            left_end -= 1;
        }
        let mut right_start = index + 1;
        while characters.get(right_start) == Some(&' ') {
            right_start += 1;
        }
        if left_end == index && right_start == index + 1 {
            continue;
        }
        let mut left_start = left_end;
        while left_start > 0 && characters[left_start - 1].is_ascii_alphabetic() {
            left_start -= 1;
        }
        let mut right_end = right_start;
        while characters
            .get(right_end)
            .is_some_and(char::is_ascii_alphabetic)
        {
            right_end += 1;
        }
        // A matching suffix inside an identifier, hyphenated token, or a pair
        // of quotation marks is not enough evidence for a prose contraction.
        if (left_start > 0 && token_character(characters[left_start - 1]))
            || characters
                .get(right_end)
                .is_some_and(|next| token_character(*next))
            || characters[..left_start]
                .iter()
                .rev()
                .find(|&&c| c != ' ')
                .is_some_and(|&c| apostrophe(c))
            || characters[right_end..]
                .iter()
                .find(|&&c| c != ' ')
                .is_some_and(|&c| apostrophe(c))
        {
            continue;
        }
        let base: String = characters[left_start..left_end].iter().collect();
        let suffix: String = characters[right_start..right_end].iter().collect();
        if known_contraction(&base, &suffix) {
            omit[left_end..index].fill(true);
            omit[index + 1..right_start].fill(true);
        }
    }
    characters
        .into_iter()
        .enumerate()
        .filter_map(|(index, character)| (!omit[index]).then_some(character))
        .collect()
}

/// Keep contraction matching inside whole words, including non-ASCII neighbors.
fn token_character(character: char) -> bool {
    character.is_alphanumeric() || matches!(character, '_' | '-')
}

/// Treat both curly directions as possible quote delimiters; only the right
/// curly apostrophe is accepted as a contraction marker by the repair caller.
fn apostrophe(character: char) -> bool {
    matches!(character, '\'' | '‘' | '’')
}

/// Explicit common English forms constrain repairs; arbitrary possessives and
/// unusual token fragments remain untouched rather than being guessed.
fn known_contraction(base: &str, suffix: &str) -> bool {
    let base = base.to_ascii_lowercase();
    let suffix = suffix.to_ascii_lowercase();
    match suffix.as_str() {
        "m" => base == "i",
        "re" => matches!(base.as_str(), "you" | "we" | "they"),
        "ve" => matches!(
            base.as_str(),
            "i" | "you" | "we" | "they" | "could" | "should" | "would" | "might" | "must"
        ),
        "ll" | "d" => matches!(
            base.as_str(),
            "i" | "you" | "he" | "she" | "it" | "we" | "they" | "that" | "there" | "who"
        ),
        "s" => matches!(
            base.as_str(),
            "it" | "he"
                | "she"
                | "that"
                | "there"
                | "what"
                | "who"
                | "where"
                | "when"
                | "how"
                | "here"
                | "let"
        ),
        "t" => matches!(
            base.as_str(),
            "can"
                | "won"
                | "shan"
                | "don"
                | "doesn"
                | "didn"
                | "isn"
                | "aren"
                | "wasn"
                | "weren"
                | "hasn"
                | "haven"
                | "hadn"
                | "couldn"
                | "shouldn"
                | "wouldn"
                | "mustn"
                | "mightn"
                | "needn"
                | "daren"
        ),
        _ => false,
    }
}
