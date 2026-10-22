//! A highlighted window of a matching text around its first occurrence.

use std::ops::Range;

use crate::search::{SearchFragment, SearchFragmentSegment};

use super::pattern::character_offsets;

/// Characters a fragment keeps, counted in code points, unless the
/// occurrence alone is longer.
pub const FRAGMENT_CHARACTERS: usize = 240;

/// Marks the side where a fragment cuts its text.
const ELLIPSIS: &str = "…";

/// The window of `text` centered on the first of `occurrences`, which are
/// sorted, non-overlapping character ranges, with every occurrence in the
/// window highlighted. An occurrence the window cuts is highlighted where
/// it is visible. A cut side starts or ends with `…`. None without an
/// occurrence.
#[must_use]
pub fn fragment(text: &str, occurrences: &[Range<usize>]) -> Option<SearchFragment> {
    let first = occurrences.first()?;
    let characters = text.chars().count();
    let window = window(first, characters);
    let visible = occurrences
        .iter()
        .filter(|occurrence| occurrence.start < window.end && occurrence.end > window.start)
        .map(|occurrence| occurrence.start.max(window.start)..occurrence.end.min(window.end))
        .collect::<Vec<_>>();
    let boundaries = std::iter::once(window.start)
        .chain(visible.iter().flat_map(|range| [range.start, range.end]))
        .chain(std::iter::once(window.end));
    let bytes = character_offsets(text, boundaries, char::len_utf8);
    let (window_start, rest) = bytes.split_first()?;
    let (window_end, highlights) = rest.split_last()?;

    let mut segments = Vec::new();
    let mut plain_start = *window_start;
    let (highlights, _) = highlights.as_chunks::<2>();
    for &[start, end] in highlights {
        push_segment(&mut segments, text.get(plain_start..start)?, false);
        push_segment(&mut segments, text.get(start..end)?, true);
        plain_start = end;
    }
    push_segment(&mut segments, text.get(plain_start..*window_end)?, false);
    if window.start > 0 {
        match segments.first_mut() {
            Some(segment) if !segment.highlighted => segment.text.insert_str(0, ELLIPSIS),
            _ => segments.insert(0, plain(ELLIPSIS)),
        }
    }
    if window.end < characters {
        match segments.last_mut() {
            Some(segment) if !segment.highlighted => segment.text.push_str(ELLIPSIS),
            _ => segments.push(plain(ELLIPSIS)),
        }
    }
    Some(SearchFragment { segments })
}

/// About [`FRAGMENT_CHARACTERS`] characters with `occurrence` in the middle,
/// shifted inward where the text ends sooner on one side.
fn window(occurrence: &Range<usize>, characters: usize) -> Range<usize> {
    let length = occurrence.end.saturating_sub(occurrence.start);
    let width = FRAGMENT_CHARACTERS.max(length);
    let before = width
        .saturating_sub(length)
        .checked_div(2)
        .unwrap_or_default();
    let start = occurrence.start.saturating_sub(before);
    let end = characters.min(start.saturating_add(width));
    end.saturating_sub(width).min(start)..end
}

fn push_segment(segments: &mut Vec<SearchFragmentSegment>, text: &str, highlighted: bool) {
    if !text.is_empty() {
        segments.push(SearchFragmentSegment {
            text: text.to_owned(),
            highlighted,
        });
    }
}

fn plain(text: &str) -> SearchFragmentSegment {
    SearchFragmentSegment {
        text: text.to_owned(),
        highlighted: false,
    }
}

#[cfg(test)]
mod tests {
    use super::{FRAGMENT_CHARACTERS, fragment};
    use crate::search::{SearchFragment, matching::pattern::Pattern};

    fn render(fragment: &SearchFragment) -> String {
        fragment
            .segments
            .iter()
            .map(|segment| {
                if segment.highlighted {
                    format!("[{}]", segment.text)
                } else {
                    segment.text.clone()
                }
            })
            .collect()
    }

    fn fragment_of(text: &str, query: &str) -> SearchFragment {
        fragment(text, &Pattern::new(query).occurrences(text)).expect("the text matches")
    }

    #[test]
    fn a_short_text_is_kept_whole_with_every_occurrence_highlighted() {
        assert_eq!(
            render(&fragment_of("Find the Needle, then the needle.", "needle")),
            "Find the [Needle], then the [needle]."
        );
        assert_eq!(render(&fragment_of("needle", "needle")), "[needle]");
        assert_eq!(fragment("no match", &[]), None);
    }

    #[test]
    fn a_long_text_is_cut_around_the_first_occurrence() {
        let text = format!("{}needle{}", "a".repeat(500), "b".repeat(500));
        let rendered = render(&fragment_of(&text, "needle"));
        // (240 - 6) / 2 characters on each side.
        let half = 117;
        assert_eq!(
            rendered,
            format!("…{}[needle]{}…", "a".repeat(half), "b".repeat(half))
        );
        let shown = rendered
            .chars()
            .filter(|&character| character != '…')
            .count();
        assert_eq!(
            shown,
            FRAGMENT_CHARACTERS + 2,
            "two brackets around the highlight"
        );
    }

    #[test]
    fn the_window_shifts_inward_at_either_end() {
        let start = format!("needle{}", "b".repeat(500));
        assert_eq!(
            render(&fragment_of(&start, "needle")),
            format!("[needle]{}…", "b".repeat(FRAGMENT_CHARACTERS - 6))
        );
        let end = format!("{}needle", "会".repeat(500));
        assert_eq!(
            render(&fragment_of(&end, "needle")),
            format!("…{}[needle]", "会".repeat(FRAGMENT_CHARACTERS - 6)),
            "the window counts code points"
        );
    }

    #[test]
    fn later_occurrences_in_the_window_are_highlighted_and_cut_ones_partly() {
        let text = format!(
            "{}needle{}needle{}",
            "x".repeat(200),
            "b".repeat(114),
            "c".repeat(100)
        );
        // The window is [83, 323): it ends inside the second occurrence,
        // which starts at 320.
        let rendered = render(&fragment_of(&text, "needle"));
        assert!(rendered.starts_with('…'), "{rendered}");
        assert!(rendered.contains("[needle]"), "{rendered}");
        assert!(rendered.ends_with("[nee]…"), "{rendered}");
    }

    #[test]
    fn an_occurrence_longer_than_the_window_is_shown_whole() {
        let query = "q".repeat(FRAGMENT_CHARACTERS + 10);
        let text = format!("before {query} after");
        assert_eq!(render(&fragment_of(&text, &query)), format!("…[{query}]…"));
    }

    #[test]
    fn highlights_follow_the_original_case_and_astral_characters() {
        assert_eq!(
            render(&fragment_of("😀 AETHER.aucept.in 😀", "aether.AUCEPT.in")),
            "😀 [AETHER.aucept.in] 😀"
        );
    }
}
