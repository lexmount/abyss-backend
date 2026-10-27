//! A folded query: the trigram expression that narrows its candidate texts,
//! and its occurrences in a text.

use std::{collections::HashSet, ops::Range};

use super::fold::fold;

/// Characters in one trigram of the candidate index.
const TRIGRAM_CHARACTERS: usize = 3;

/// A nonempty query folded for matching.
pub struct Pattern {
    folded: String,
    characters: usize,
}

impl Pattern {
    /// Folds a validated, nonempty query.
    #[must_use]
    pub fn new(query: &str) -> Self {
        let folded = fold(query);
        let characters = folded.chars().count();
        Self { folded, characters }
    }

    /// An FTS5 expression over a trigram index of folded texts that every
    /// text holding the query matches: each distinct trigram of the folded
    /// query as its own quoted string, `"` written as `""`, joined with
    /// `AND`. None when the query is shorter than a trigram; every text is
    /// then a candidate.
    ///
    /// A trigram holding NUL is left out, because an FTS5 string ends there;
    /// leaving a trigram out only widens the candidates.
    #[must_use]
    pub fn trigram_expression(&self) -> Option<String> {
        let characters = self.folded.chars().collect::<Vec<_>>();
        let mut seen = HashSet::new();
        let terms = characters
            .windows(TRIGRAM_CHARACTERS)
            .filter(|trigram| !trigram.contains(&'\0'))
            .map(|trigram| trigram.iter().collect::<String>())
            .filter(|trigram| seen.insert(trigram.clone()))
            .map(|trigram| format!("\"{}\"", trigram.replace('"', "\"\"")))
            .collect::<Vec<_>>();
        (!terms.is_empty()).then(|| terms.join(" AND "))
    }

    /// Whether the folded `text` holds the folded query.
    #[must_use]
    pub fn matches(&self, text: &str) -> bool {
        fold(text).contains(self.folded.as_str())
    }

    /// Non-overlapping occurrences of the query in `text`, left to right, as
    /// character ranges. Folding keeps the character count, so the ranges
    /// found in the folded text are the same ranges of `text`.
    #[must_use]
    pub fn occurrences(&self, text: &str) -> Vec<Range<usize>> {
        let folded = fold(text);
        let mut ranges = Vec::new();
        let mut scanned_bytes = 0_usize;
        let mut scanned_characters = 0_usize;
        for (start, _) in folded.match_indices(self.folded.as_str()) {
            scanned_characters = scanned_characters.saturating_add(
                folded
                    .get(scanned_bytes..start)
                    .map_or(0, |skipped| skipped.chars().count()),
            );
            scanned_bytes = start;
            ranges.push(scanned_characters..scanned_characters.saturating_add(self.characters));
        }
        ranges
    }
}

/// Character ranges of `text`, sorted and non-overlapping, as half-open
/// ranges of its UTF-16 code units.
#[must_use]
pub fn utf16_ranges(text: &str, ranges: &[Range<usize>]) -> Vec<[usize; 2]> {
    let offsets = character_offsets(
        text,
        ranges.iter().flat_map(|range| [range.start, range.end]),
        char::len_utf16,
    );
    let (pairs, _) = offsets.as_chunks::<2>();
    pairs.to_vec()
}

/// The offset of each of `positions`, character positions in non-decreasing
/// order, counted in the units `width` gives each character of `text`.
pub fn character_offsets<P: IntoIterator<Item = usize>>(
    text: &str,
    positions: P,
    width: fn(char) -> usize,
) -> Vec<usize> {
    let mut characters = text.chars();
    let mut position = 0_usize;
    let mut offset = 0_usize;
    positions
        .into_iter()
        .map(|target| {
            while position < target {
                let Some(character) = characters.next() else {
                    break;
                };
                offset = offset.saturating_add(width(character));
                position = position.saturating_add(1);
            }
            offset
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{Pattern, character_offsets, utf16_ranges};

    #[test]
    fn trigram_expression_quotes_each_distinct_trigram() {
        assert_eq!(
            Pattern::new("Cargo").trigram_expression().as_deref(),
            Some(r#""car" AND "arg" AND "rgo""#)
        );
        assert_eq!(
            Pattern::new("aaaa").trigram_expression().as_deref(),
            Some(r#""aaa""#),
            "repeated trigrams appear once"
        );
        assert_eq!(
            Pattern::new(r#"a"b"#).trigram_expression().as_deref(),
            Some(r#""a""b""#),
            "a double quote is written twice inside the string"
        );
        assert_eq!(
            Pattern::new("新起一条流").trigram_expression().as_deref(),
            Some(r#""新起一" AND "起一条" AND "一条流""#)
        );
        assert_eq!(
            Pattern::new("a b").trigram_expression().as_deref(),
            Some(r#""a b""#),
            "spaces are characters of the query"
        );
    }

    #[test]
    fn short_queries_have_no_trigram_and_scan_every_text() {
        assert_eq!(Pattern::new("搜").trigram_expression(), None);
        assert_eq!(Pattern::new("搜索").trigram_expression(), None);
        assert_eq!(Pattern::new("a\0").trigram_expression(), None);
        assert_eq!(
            Pattern::new("ab\0cd").trigram_expression(),
            None,
            "every trigram holds NUL"
        );
        assert_eq!(
            Pattern::new("abc\0d").trigram_expression().as_deref(),
            Some(r#""abc""#)
        );
    }

    #[test]
    fn matches_after_folding_both_sides() {
        let pattern = Pattern::new("ÄBC");
        assert!(pattern.matches("xxäbcxx"));
        assert!(pattern.matches("ÄBC"));
        assert!(!pattern.matches("abc"));
        assert!(Pattern::new("straße").matches("STRAßE"));
        assert!(!Pattern::new("strasse").matches("STRAßE"));
        assert!(Pattern::new("cargo test").matches("run `CARGO TEST` now"));
        assert!(!Pattern::new("cargo test").matches("cargo  test"));
    }

    #[test]
    fn occurrences_do_not_overlap() {
        assert_eq!(Pattern::new("aa").occurrences("aaaaa"), [0..2, 2..4]);
        let once = Pattern::new("aba").occurrences("ababa");
        assert_eq!((once.len(), once.first()), (1, Some(&(0..3))));
        assert_eq!(
            Pattern::new("x").occurrences("abc"),
            Vec::<std::ops::Range<usize>>::new()
        );
        // The Kelvin sign folds to an ASCII k, so byte positions shift while
        // character positions do not.
        assert_eq!(Pattern::new("k").occurrences("\u{212a}ok"), [0..1, 2..3]);
        assert_eq!(
            Pattern::new("搜索").occurrences("先搜索，再搜索"),
            [1..3, 5..7]
        );
    }

    #[test]
    fn utf16_ranges_count_astral_characters_twice() {
        let text = "😀 aether 😀AETHER";
        let ranges = Pattern::new("aether").occurrences(text);
        assert_eq!(ranges, [2..8, 10..16]);
        let utf16 = utf16_ranges(text, &ranges);
        assert_eq!(utf16, [[3, 9], [12, 18]]);
        let units = text.encode_utf16().collect::<Vec<_>>();
        for [start, end] in utf16 {
            assert_eq!(
                String::from_utf16(&units[start..end])
                    .expect("ranges fall on character boundaries")
                    .to_lowercase(),
                "aether"
            );
        }
        assert_eq!(
            utf16_ranges("İx", &Pattern::new("x").occurrences("İx")),
            [[1, 2]]
        );
    }

    #[test]
    fn character_offsets_follow_the_unit_width() {
        let text = "a会😀b";
        assert_eq!(
            character_offsets(text, [0, 1, 2, 3, 4], char::len_utf8),
            [0, 1, 4, 8, 9]
        );
        assert_eq!(
            character_offsets(text, [0, 1, 2, 3, 4], char::len_utf16),
            [0, 1, 2, 4, 5]
        );
        assert_eq!(
            character_offsets(text, [9], char::len_utf8),
            [9],
            "past the end stops at it"
        );
    }
}
