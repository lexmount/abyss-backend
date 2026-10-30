//! Previews of one event's conversation: the chosen runs of its entries, each
//! text cut to a length, with what was left out counted.
//!
//! Entry indexes are those of the event's full entry list, so a position found
//! elsewhere, such as a search hit, points into a preview unchanged.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use super::{UsageEventResponse, attachments::UsageEventAttachmentResponse};
use crate::error::AppError;

#[derive(Debug, Default, Deserialize)]
/// Which entries of a conversation to give, and how much of each text.
pub struct PreviewQuery {
    /// Comma-separated runs of entries: `a-b` from index `a` to `b`, `-n` the
    /// last `n`, `a` the one at `a`. All entries without it.
    pub entries: Option<String>,
    /// Most UTF-16 code units of each entry text. Whole texts without it.
    pub text_preview: Option<usize>,
}

/// One run of entries as a query asks for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Run {
    /// Entries `from` through `to`.
    Span { from: usize, to: usize },
    /// The last `count` entries.
    Last { count: usize },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// A validated [`PreviewQuery`].
pub struct Window {
    runs: Option<Vec<Run>>,
    text_preview: Option<usize>,
}

impl Window {
    /// Validates a query; a malformed run is a validation error.
    pub fn parse(query: &PreviewQuery) -> Result<Self, AppError> {
        let runs = query
            .entries
            .as_deref()
            .map(|entries| entries.split(',').map(str::trim).map(parse_run).collect())
            .transpose()?;
        Ok(Self {
            runs,
            text_preview: query.text_preview,
        })
    }

    /// The runs over `total` entries as sorted, disjoint index ranges.
    fn ranges(&self, total: usize) -> Vec<std::ops::Range<usize>> {
        let Some(runs) = &self.runs else {
            return std::iter::once(0..total).collect();
        };
        let mut ranges = runs
            .iter()
            .map(|run| match *run {
                Run::Span { from, to } => from.min(total)..to.saturating_add(1).min(total),
                Run::Last { count } => total.saturating_sub(count)..total,
            })
            .filter(|range| !range.is_empty())
            .collect::<Vec<_>>();
        ranges.sort_by_key(|range| range.start);
        let mut merged: Vec<std::ops::Range<usize>> = Vec::new();
        for range in ranges {
            match merged.last_mut() {
                Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
                _ => merged.push(range),
            }
        }
        merged
    }
}

fn parse_run(run: &str) -> Result<Run, AppError> {
    let invalid = || AppError::validation(format!("invalid entries run: {run:?}"));
    let index = |text: &str| text.parse::<usize>().map_err(|_| invalid());
    if let Some(count) = run.strip_prefix('-') {
        return Ok(Run::Last {
            count: index(count)?,
        });
    }
    let (from, to) = if let Some((from, to)) = run.split_once('-') {
        (index(from)?, index(to)?)
    } else {
        let at = index(run)?;
        (at, at)
    };
    if from > to {
        return Err(invalid());
    }
    Ok(Run::Span { from, to })
}

#[derive(Debug, Serialize)]
/// The chosen part of one event's conversation.
pub struct ConversationPreview {
    /// Event primary key.
    pub event_pk: Uuid,
    /// Completeness the collector reported for the conversation.
    pub completeness: Option<String>,
    /// Number of entries in the full conversation.
    pub entries_total: usize,
    /// The chosen runs in order.
    pub chunks: Vec<Chunk>,
    /// Number of entries no chunk holds.
    pub omitted_entries: usize,
    /// UTF-8 bytes of the texts of those entries.
    pub omitted_bytes: usize,
    /// Image attachments of the event.
    pub attachments: Vec<UsageEventAttachmentResponse>,
}

#[derive(Debug, Serialize)]
/// One run of consecutive entries.
pub struct Chunk {
    /// Index of the first entry in the full conversation.
    pub from: usize,
    /// The entries as the collector reported them. A text cut to the
    /// preview length carries `text_length`, its full length in UTF-16 code
    /// units.
    pub entries: Vec<Value>,
}

/// Previews the conversation of `event` through `window`.
pub fn preview(event: UsageEventResponse, window: &Window) -> ConversationPreview {
    let conversation = event.conversation.as_ref();
    let entries = conversation
        .and_then(|conversation| conversation.get("entries"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let ranges = window.ranges(entries.len());
    let shown = ranges.iter().map(std::ops::Range::len).sum::<usize>();
    let omitted_bytes = entries
        .iter()
        .enumerate()
        .filter(|(index, _)| !ranges.iter().any(|range| range.contains(index)))
        .map(|(_, entry)| text_of(entry).len())
        .sum();
    let completeness = conversation
        .and_then(|conversation| conversation.get("completeness"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let chunks = ranges
        .into_iter()
        .map(|range| Chunk {
            from: range.start,
            entries: entries[range]
                .iter()
                .map(|entry| cut(entry, window.text_preview))
                .collect(),
        })
        .collect();
    let entries_total = entries.len();
    ConversationPreview {
        event_pk: event.id,
        completeness,
        entries_total,
        chunks,
        omitted_entries: entries_total.saturating_sub(shown),
        omitted_bytes,
        attachments: event.attachments,
    }
}

fn text_of(entry: &Value) -> &str {
    entry
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// `entry` with its text cut to `limit` UTF-16 code units, never inside a
/// character.
fn cut(entry: &Value, limit: Option<usize>) -> Value {
    let mut entry = entry.clone();
    let Some(limit) = limit else {
        return entry;
    };
    let text = text_of(&entry);
    let length = text.encode_utf16().count();
    if length <= limit {
        return entry;
    }
    let mut units = 0_usize;
    let end = text
        .char_indices()
        .find(|(_, character)| {
            units = units.saturating_add(character.len_utf16());
            units > limit
        })
        .map_or(text.len(), |(at, _)| at);
    let kept = text[..end].to_owned();
    if let Some(object) = entry.as_object_mut() {
        object.insert("text".to_owned(), Value::String(kept));
        object.insert("text_length".to_owned(), Value::from(length));
    }
    entry
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{PreviewQuery, Window, cut};

    fn window(entries: Option<&str>, text_preview: Option<usize>) -> Window {
        Window::parse(&PreviewQuery {
            entries: entries.map(str::to_owned),
            text_preview,
        })
        .expect("a valid window")
    }

    #[test]
    fn runs_resolve_to_sorted_disjoint_ranges_within_the_entries() {
        /// Entries run, entry count, and the expected `(start, end)` ranges.
        type Case<'a> = (Option<&'a str>, usize, &'a [(usize, usize)]);
        let cases: [Case<'_>; 6] = [
            (None, 4, &[(0, 4)]),
            (Some("0-2,-5"), 10, &[(0, 3), (5, 10)]),
            (Some("0-2,-5"), 6, &[(0, 6)]),
            (Some("-2,3"), 10, &[(3, 4), (8, 10)]),
            (Some("0-99"), 3, &[(0, 3)]),
            (Some("7"), 3, &[]),
        ];
        for (entries, total, expected) in cases {
            let ranges = window(entries, None)
                .ranges(total)
                .into_iter()
                .map(|range| (range.start, range.end))
                .collect::<Vec<_>>();
            assert_eq!(ranges, expected, "{entries:?} of {total}");
        }
        for invalid in ["", "a", "3-1", "1-", "-"] {
            assert!(
                Window::parse(&PreviewQuery {
                    entries: Some(invalid.to_owned()),
                    text_preview: None,
                })
                .is_err(),
                "{invalid:?}"
            );
        }
    }

    #[test]
    fn long_texts_keep_their_start_and_full_length() {
        let entry = json!({"role": "user", "text": "ab😀cd"});
        assert_eq!(cut(&entry, None), entry);
        assert_eq!(cut(&entry, Some(6)), entry, "six UTF-16 code units fit");
        let cut_at = |limit| {
            let cut = cut(&entry, Some(limit));
            (cut["text"].clone(), cut["text_length"].clone())
        };
        assert_eq!(
            cut_at(3),
            (json!("ab"), json!(6_usize)),
            "never half a character"
        );
        assert_eq!(cut_at(4), (json!("ab😀"), json!(6_usize)));
        assert_eq!(
            cut(&json!({"role": "user"}), Some(1)),
            json!({"role": "user"})
        );
        assert_eq!(cut(&Value::Null, Some(1)), Value::Null);
    }
}
