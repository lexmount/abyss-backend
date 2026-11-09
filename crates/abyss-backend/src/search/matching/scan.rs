//! Bounded in-session matching of ordered event conversations. Text ids are
//! local to the response; identical texts share their occurrence ranges.

use std::collections::HashMap;

use serde_json::Value;
use uuid::Uuid;

use super::{
    in_session::MAX_IN_SESSION_HITS,
    pattern::{Pattern, utf16_ranges},
    pieces::pieces,
};
use crate::{
    error::AppError,
    search::{InSessionHit, InSessionSearchResponse},
    usage::UsageEventType,
};

/// Accumulates at most one hit beyond the response limit to detect truncation.
pub struct SessionScan {
    query: String,
    pattern: Pattern,
    text_ids: HashMap<String, i64>,
    ranges: HashMap<i64, Vec<[usize; 2]>>,
    hits: Vec<InSessionHit>,
}

impl SessionScan {
    /// Starts a scan with an already validated query.
    pub fn new(query: String) -> Self {
        Self {
            pattern: Pattern::new(&query),
            query,
            text_ids: HashMap::new(),
            ranges: HashMap::new(),
            hits: Vec::new(),
        }
    }

    /// Adds one event in observation/id order, returning true once the scan
    /// has proved that more hits exist than can be returned.
    pub fn push(
        &mut self,
        event_pk: Uuid,
        event_type: UsageEventType,
        new_from: Option<u32>,
        conversation: Option<&Value>,
    ) -> Result<bool, AppError> {
        for piece in pieces(conversation) {
            if self.hits.len() > MAX_IN_SESSION_HITS {
                return Ok(true);
            }
            let Some(text_id) = self.matching_text(piece.text)? else {
                continue;
            };
            let entry_index = u32::try_from(piece.entry_index)
                .map_err(|error| AppError::internal(format!("entry index: {error}")))?;
            self.hits.push(InSessionHit {
                event_pk,
                entry_index,
                field: piece.field,
                new: event_type == UsageEventType::Response
                    || entry_index >= new_from.unwrap_or_default(),
                text_id,
                segment: None,
            });
        }
        Ok(self.hits.len() > MAX_IN_SESSION_HITS)
    }

    fn matching_text(&mut self, text: &str) -> Result<Option<i64>, AppError> {
        if let Some(id) = self.text_ids.get(text) {
            return Ok(Some(*id));
        }
        let occurrences = self.pattern.occurrences(text);
        if occurrences.is_empty() {
            return Ok(None);
        }
        let id = i64::try_from(self.text_ids.len())
            .map_err(|error| AppError::internal(format!("search text id: {error}")))?;
        self.text_ids.insert(text.to_owned(), id);
        self.ranges.insert(id, utf16_ranges(text, &occurrences));
        Ok(Some(id))
    }

    /// Returns only the bounded hits and the texts those hits name.
    pub fn finish(self) -> InSessionSearchResponse {
        InSessionSearchResponse::from_hits(self.query, self.hits, self.ranges)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{MAX_IN_SESSION_HITS, SessionScan};
    use crate::{search::PieceField, usage::UsageEventType};
    use uuid::Uuid;

    #[test]
    fn preserves_entry_order_utf16_ranges_and_new_content() {
        let mut scan = SessionScan::new("NIMBUS".to_owned());
        let event = json!({"entries": [
            {"role": "assistant", "text": "😀 nimbus 😀NIMBUS"},
            {"role": "tool_call", "name": "nimbus", "text": "nimbus"},
            {"role": "user", "text": "😀 nimbus 😀NIMBUS"}
        ]});
        let event_pk = Uuid::from_u128(1);
        assert!(
            !scan
                .push(event_pk, UsageEventType::Request, Some(1), Some(&event))
                .unwrap()
        );
        let response = scan.finish();
        let positions = response
            .hits
            .iter()
            .map(|hit| (hit.entry_index, hit.field, hit.new))
            .collect::<Vec<_>>();
        assert_eq!(
            positions,
            [
                (0, PieceField::Text, false),
                (1, PieceField::Name, true),
                (1, PieceField::Text, true),
                (2, PieceField::Text, true)
            ]
        );
        assert_eq!(response.hits[0].text_id, response.hits[3].text_id);
        assert_eq!(response.hits[1].text_id, response.hits[2].text_id);
        assert_eq!(response.texts.len(), 2);
        assert_eq!(response.texts[0].ranges, [[3, 9], [12, 18]]);
        assert!(!response.truncated);
    }

    #[test]
    fn responses_and_missing_facts_are_new_and_entries_are_not_joined() {
        let mut scan = SessionScan::new("abc".to_owned());
        let event = json!({"entries": [{"text": "abc"}, {"text": "ab"}, {"text": "c"}]});
        for (side, new_from) in [
            (UsageEventType::Response, Some(3)),
            (UsageEventType::Request, None),
        ] {
            assert!(
                !scan
                    .push(Uuid::from_u128(1), side, new_from, Some(&event))
                    .unwrap()
            );
        }
        assert!(
            !scan
                .push(Uuid::from_u128(2), UsageEventType::Request, None, None)
                .unwrap()
        );
        let response = scan.finish();
        assert_eq!(response.hits.len(), 2);
        assert!(response.hits.iter().all(|hit| hit.new));
        assert_eq!(response.texts.len(), 1);
    }

    #[test]
    fn stops_only_after_the_first_excess_hit_and_drops_its_text() {
        let event = json!({"entries": vec![json!({"text": "x"}); MAX_IN_SESSION_HITS]});
        let mut scan = SessionScan::new("x".to_owned());
        assert!(
            !scan
                .push(
                    Uuid::from_u128(1),
                    UsageEventType::Response,
                    None,
                    Some(&event)
                )
                .unwrap()
        );
        assert!(
            scan.push(
                Uuid::from_u128(2),
                UsageEventType::Response,
                None,
                Some(&json!({"entries": [{"text": "xx"}]}))
            )
            .unwrap()
        );
        let response = scan.finish();
        assert_eq!(response.hits.len(), MAX_IN_SESSION_HITS);
        assert!(response.truncated);
        assert_eq!(response.texts.len(), 1);
    }
}
