//! Per-character case folding applied alike to texts and queries.

/// One character's lowercase form when that is exactly one character, else
/// the character itself.
#[must_use]
pub fn fold_char(character: char) -> char {
    if character.is_ascii() {
        return character.to_ascii_lowercase();
    }
    let mut lowercase = character.to_lowercase();
    match (lowercase.next(), lowercase.next()) {
        (Some(folded), None) => folded,
        _ => character,
    }
}

/// Folds every character of `text`. The result has as many characters as
/// `text`, so a character position in it is the same position in `text`.
#[must_use]
pub fn fold(text: &str) -> String {
    if text.is_ascii() {
        return text.to_ascii_lowercase();
    }
    text.chars().map(fold_char).collect()
}

#[cfg(test)]
mod tests {
    use super::fold;

    #[test]
    fn folds_case_one_character_at_a_time() {
        assert_eq!(fold("ÄBC"), fold("äbc"));
        assert_eq!(fold("ÄBC"), "äbc");
        assert_eq!(
            fold("ΣΊΣΥΦΟΣ"),
            "σίσυφοσ",
            "Greek folds without final-sigma context"
        );
        assert_eq!(fold("Cargo TEST"), "cargo test");
        assert_eq!(fold("搜索 STATUS.md"), "搜索 status.md");
    }

    #[test]
    fn keeps_characters_whose_lowercase_is_not_one_character() {
        // İ lowercases to two characters, i and a combining dot.
        assert_eq!(fold("İstanbul"), "İstanbul");
        assert_eq!(fold("ß"), "ß");
        assert_ne!(fold("Straße"), fold("STRASSE"), "ß does not expand to ss");
        assert_eq!(
            fold("ẞ"),
            "ß",
            "capital sharp s lowercases to one character"
        );
    }

    #[test]
    fn leaves_width_and_compatibility_forms_alone() {
        assert_eq!(
            fold("ＡＢＣ"),
            "ａｂｃ",
            "full-width letters fold within their width"
        );
        assert_ne!(fold("ＡＢＣ"), fold("abc"));
        assert_ne!(fold("ﬁ"), fold("fi"));
        assert_eq!(fold("😀 Ü"), "😀 ü");
    }

    #[test]
    fn keeps_the_character_count() {
        for text in ["İİ", "ΣΑΣ", "Kelvin K", "ǅ", "😀a", ""] {
            assert_eq!(fold(text).chars().count(), text.chars().count(), "{text}");
        }
    }
}
