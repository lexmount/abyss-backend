//! The bounded response of a search within one session.

use std::collections::{BTreeSet, HashMap};

use crate::search::{InSessionHit, InSessionSearchResponse, MatchedText};

/// Hits a search within one session returns at most.
pub const MAX_IN_SESSION_HITS: usize = 100_000;

impl InSessionSearchResponse {
    /// Keeps the first [`MAX_IN_SESSION_HITS`] of `hits`, which are in
    /// response order and hold at most one more, with the occurrence ranges
    /// of each text they name, ordered by text id.
    #[must_use]
    pub fn from_hits(
        query: String,
        mut hits: Vec<InSessionHit>,
        mut ranges: HashMap<i64, Vec<[usize; 2]>>,
    ) -> Self {
        let truncated = hits.len() > MAX_IN_SESSION_HITS;
        hits.truncate(MAX_IN_SESSION_HITS);
        let texts = hits
            .iter()
            .map(|hit| hit.text_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|id| {
                Some(MatchedText {
                    id,
                    ranges: ranges.remove(&id)?,
                })
            })
            .collect();
        Self {
            query,
            texts,
            hits,
            truncated,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;
    use uuid::Uuid;

    use super::MAX_IN_SESSION_HITS;
    use crate::search::{InSessionHit, InSessionSearchResponse, MatchedText, PieceField};

    fn hit(text_id: i64) -> InSessionHit {
        InSessionHit {
            event_pk: Uuid::from_u128(1),
            entry_index: 0,
            field: PieceField::Text,
            new: true,
            text_id,
            segment: Some("e1".to_owned()),
        }
    }

    #[test]
    fn names_only_the_texts_of_the_returned_hits() {
        let response = InSessionSearchResponse::from_hits(
            "aether".to_owned(),
            vec![hit(12), hit(3), hit(12)],
            HashMap::from([
                (3, vec![[0, 6]]),
                (12, vec![[3, 9], [40, 46]]),
                (99, vec![[0, 1]]),
            ]),
        );
        assert!(!response.truncated);
        assert_eq!(
            response.texts,
            [
                MatchedText {
                    id: 3,
                    ranges: vec![[0, 6]]
                },
                MatchedText {
                    id: 12,
                    ranges: vec![[3, 9], [40, 46]]
                }
            ]
        );
        assert_eq!(
            serde_json::to_value(&response).expect("serializes"),
            json!({
                "query": "aether",
                "texts": [{"id": 3_i64, "ranges": [[0_i64, 6_i64]]}, {"id": 12_i64, "ranges": [[3_i64, 9_i64], [40_i64, 46_i64]]}],
                "hits": [
                    {"event_pk": Uuid::from_u128(1), "entry_index": 0_i64, "field": "text", "new": true, "text_id": 12_i64, "segment": "e1"},
                    {"event_pk": Uuid::from_u128(1), "entry_index": 0_i64, "field": "text", "new": true, "text_id": 3_i64, "segment": "e1"},
                    {"event_pk": Uuid::from_u128(1), "entry_index": 0_i64, "field": "text", "new": true, "text_id": 12_i64, "segment": "e1"}
                ],
                "truncated": false
            })
        );
    }

    #[test]
    fn caps_the_hits_and_reports_truncation() {
        let at_cap = InSessionSearchResponse::from_hits(
            "x".to_owned(),
            vec![hit(1); MAX_IN_SESSION_HITS],
            HashMap::from([(1, vec![[0, 1]])]),
        );
        assert_eq!(
            (at_cap.hits.len(), at_cap.truncated),
            (MAX_IN_SESSION_HITS, false)
        );

        let mut over = vec![hit(1); MAX_IN_SESSION_HITS];
        over.push(hit(2));
        let over_cap = InSessionSearchResponse::from_hits(
            "x".to_owned(),
            over,
            HashMap::from([(1, vec![[0, 1]]), (2, vec![[0, 1]])]),
        );
        assert_eq!(
            (over_cap.hits.len(), over_cap.truncated),
            (MAX_IN_SESSION_HITS, true)
        );
        assert_eq!(
            over_cap
                .texts
                .iter()
                .map(|text| text.id)
                .collect::<Vec<_>>(),
            [1],
            "a text only the dropped hit names is left out"
        );
    }
}
