//! Deterministic ordering for normalized events inside Agent timeline turns.
//!
//! Codex emits a provider response chain through `response_id` and
//! `previous_response_id`. Timeline ordering prefers that native relationship,
//! then falls back to high-precision observation time, event side, and stable
//! event identity.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
};

use chrono::{DateTime, Utc};

use crate::db::models::UsageEvent;

/// Applies the authoritative event order used by owned and shared timelines.
pub struct UsageEventTimelineOrder;

impl UsageEventTimelineOrder {
    /// Sorts events by turn, provider chain, observation time, side, and ID.
    ///
    /// The final event identifier tie-breaker makes output deterministic even
    /// for corrupt cycles or collectors with identical timestamps.
    pub fn sort(events: &mut [UsageEvent]) {
        let provider_ranks = ProviderResponseRanks::from_events(events);
        events.sort_by(|left, right| {
            left.turn_index.cmp(&right.turn_index).then_with(|| {
                provider_ranks.compare_events(left, right).then_with(|| {
                    left.observed_at
                        .cmp(&right.observed_at)
                        .then_with(|| {
                            event_side_rank(&left.event_type)
                                .cmp(&event_side_rank(&right.event_type))
                        })
                        .then_with(|| left.event_id.cmp(&right.event_id))
                })
            })
        });
    }
}

struct ProviderResponseRanks {
    ranks: HashMap<(i32, String), usize>,
    native_turns: HashSet<i32>,
}

impl ProviderResponseRanks {
    fn from_events(events: &[UsageEvent]) -> Self {
        let mut nodes_by_turn = BTreeMap::<i32, HashMap<String, ProviderResponseNode>>::new();
        // Native provider ordering is safe only when every event in a turn has
        // a response_id. A partially instrumented turn falls back as a whole so
        // ranked and unranked events cannot interleave unpredictably.
        let mut native_turns = events
            .iter()
            .map(|event| event.turn_index)
            .collect::<HashSet<_>>();
        for event in events {
            let Some(response_id) = metadata_string(&event.metadata, "response_id") else {
                native_turns.remove(&event.turn_index);
                continue;
            };
            let previous_response_id =
                metadata_string(&event.metadata, "previous_response_id").map(str::to_owned);
            let nodes = nodes_by_turn.entry(event.turn_index).or_default();
            let node =
                nodes
                    .entry(response_id.to_owned())
                    .or_insert_with(|| ProviderResponseNode {
                        response_id: response_id.to_owned(),
                        previous_response_id: previous_response_id.clone(),
                        first_observed_at: event.observed_at,
                        first_event_id: event.event_id.clone(),
                    });
            node.merge_evidence(event, previous_response_id);
        }

        let mut ranks = HashMap::new();
        for (turn_index, nodes) in nodes_by_turn {
            Self::rank_turn(turn_index, &nodes, &mut ranks);
        }
        Self {
            ranks,
            native_turns,
        }
    }

    fn rank_turn(
        turn_index: i32,
        nodes: &HashMap<String, ProviderResponseNode>,
        ranks: &mut HashMap<(i32, String), usize>,
    ) {
        let mut roots = Vec::new();
        let mut children = HashMap::<String, Vec<String>>::new();
        for node in nodes.values() {
            match node
                .previous_response_id
                .as_ref()
                .filter(|previous| previous.as_str() != node.response_id)
                .filter(|previous| nodes.contains_key(previous.as_str()))
            {
                Some(previous) => children
                    .entry(previous.clone())
                    .or_default()
                    .push(node.response_id.clone()),
                None => roots.push(node.response_id.clone()),
            }
        }

        // Branches can occur after retries or malformed evidence. Sort roots and
        // siblings by stable observed/event fallback data before traversal.
        let compare_ids = |left: &String, right: &String| {
            nodes
                .get(left)
                .expect("provider response id should resolve")
                .fallback_cmp(
                    nodes
                        .get(right)
                        .expect("provider response id should resolve"),
                )
        };
        roots.sort_by(compare_ids);
        for child_ids in children.values_mut() {
            child_ids.sort_by(compare_ids);
        }

        let mut visited = HashSet::new();
        let mut next_rank = 0_usize;
        for root in roots {
            Self::rank_chain(
                turn_index,
                root,
                &children,
                ranks,
                &mut visited,
                &mut next_rank,
            );
        }

        // Cycles have no root. Traversing remaining nodes after rooted chains
        // guarantees every response still receives a deterministic rank.
        let mut remaining = nodes
            .keys()
            .filter(|response_id| !visited.contains(response_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        remaining.sort_by(compare_ids);
        for response_id in remaining {
            Self::rank_chain(
                turn_index,
                response_id,
                &children,
                ranks,
                &mut visited,
                &mut next_rank,
            );
        }
    }

    fn rank_chain(
        turn_index: i32,
        root: String,
        children: &HashMap<String, Vec<String>>,
        ranks: &mut HashMap<(i32, String), usize>,
        visited: &mut HashSet<String>,
        next_rank: &mut usize,
    ) {
        let mut pending = vec![root];
        while let Some(response_id) = pending.pop() {
            if !visited.insert(response_id.clone()) {
                continue;
            }
            ranks.insert((turn_index, response_id.clone()), *next_rank);
            *next_rank = next_rank.saturating_add(1);
            if let Some(child_ids) = children.get(&response_id) {
                pending.extend(child_ids.iter().rev().cloned());
            }
        }
    }

    fn compare_events(&self, left: &UsageEvent, right: &UsageEvent) -> Ordering {
        if left.turn_index != right.turn_index || !self.native_turns.contains(&left.turn_index) {
            return Ordering::Equal;
        }

        let left_response_id = metadata_string(&left.metadata, "response_id");
        let right_response_id = metadata_string(&right.metadata, "response_id");

        // Request and response observations that describe the same provider
        // call remain adjacent and request-first regardless of timestamp ties.
        if left_response_id.is_some() && left_response_id == right_response_id {
            return event_side_rank(&left.event_type).cmp(&event_side_rank(&right.event_type));
        }

        let native_order = left_response_id
            .and_then(|response_id| self.ranks.get(&(left.turn_index, response_id.to_owned())))
            .zip(right_response_id.and_then(|response_id| {
                self.ranks.get(&(right.turn_index, response_id.to_owned()))
            }))
            .map(|(left_rank, right_rank)| left_rank.cmp(right_rank));
        if let Some(ordering) = native_order.filter(|ordering| !ordering.is_eq()) {
            return ordering;
        }

        Ordering::Equal
    }
}

struct ProviderResponseNode {
    response_id: String,
    previous_response_id: Option<String>,
    first_observed_at: DateTime<Utc>,
    first_event_id: String,
}

impl ProviderResponseNode {
    fn merge_evidence(&mut self, event: &UsageEvent, previous_response_id: Option<String>) {
        if self.previous_response_id.is_none() {
            self.previous_response_id = previous_response_id;
        }
        if (event.observed_at, event.event_id.as_str())
            < (self.first_observed_at, self.first_event_id.as_str())
        {
            self.first_observed_at = event.observed_at;
            self.first_event_id.clone_from(&event.event_id);
        }
    }

    fn fallback_cmp(&self, other: &Self) -> Ordering {
        self.first_observed_at
            .cmp(&other.first_observed_at)
            .then_with(|| self.first_event_id.cmp(&other.first_event_id))
            .then_with(|| self.response_id.cmp(&other.response_id))
    }
}

fn metadata_string<'a>(metadata: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    metadata
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

const fn event_side_rank(event_type: &str) -> u8 {
    match event_type.as_bytes() {
        b"request" => 0,
        b"response" => 1,
        _ => 2,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone as _, Timelike as _, Utc};
    use serde_json::json;
    use uuid::Uuid;

    use super::UsageEventTimelineOrder;
    use crate::db::models::UsageEvent;

    #[test]
    fn native_response_chain_precedes_timestamps_and_collector_ordinals() {
        let timestamp = Utc
            .with_ymd_and_hms(2026, 8, 4, 2, 12, 24)
            .single()
            .expect("timestamp should be valid");
        let mut events = vec![
            usage_event(
                "b-response",
                "response",
                timestamp,
                json!({"response_id": "resp_b", "previous_response_id": "resp_a", "provider_call_index": 1_i64}),
            ),
            usage_event(
                "a-response",
                "response",
                timestamp,
                json!({"response_id": "resp_a", "provider_call_index": 2_i64}),
            ),
            usage_event(
                "b-request",
                "request",
                timestamp,
                json!({"response_id": "resp_b", "previous_response_id": "resp_a", "provider_call_index": 1_i64}),
            ),
            usage_event(
                "a-request",
                "request",
                timestamp,
                json!({"response_id": "resp_a", "provider_call_index": 2_i64}),
            ),
        ];

        UsageEventTimelineOrder::sort(&mut events);

        assert_eq!(
            event_ids(&events),
            ["a-request", "a-response", "b-request", "b-response"]
        );
    }

    #[test]
    fn timestamp_precedes_collector_ordinal_when_native_chain_is_incomplete() {
        let first_timestamp = Utc
            .with_ymd_and_hms(2026, 8, 4, 2, 12, 24)
            .single()
            .expect("timestamp should be valid");
        let second_timestamp = first_timestamp + chrono::TimeDelta::microseconds(1);
        let mut events = vec![
            usage_event(
                "later-despite-smaller-ordinal",
                "response",
                second_timestamp,
                json!({"provider_call_index": 1_i64, "response_id": "resp_later"}),
            ),
            usage_event(
                "earlier-despite-larger-ordinal",
                "response",
                first_timestamp,
                json!({"provider_call_index": 2_i64}),
            ),
        ];

        UsageEventTimelineOrder::sort(&mut events);

        assert_eq!(
            event_ids(&events),
            [
                "earlier-despite-larger-ordinal",
                "later-despite-smaller-ordinal"
            ]
        );
    }

    #[test]
    fn high_precision_timestamp_orders_legacy_events_without_provider_evidence() {
        let first = Utc
            .with_ymd_and_hms(2026, 8, 4, 2, 12, 24)
            .single()
            .expect("timestamp should be valid")
            .with_nanosecond(1_000)
            .expect("microsecond should be valid");
        let second = first
            .with_nanosecond(2_000)
            .expect("microsecond should be valid");
        let mut events = vec![
            usage_event("second", "request", second, json!({})),
            usage_event("first", "response", first, json!({})),
        ];

        UsageEventTimelineOrder::sort(&mut events);

        assert_eq!(event_ids(&events), ["first", "second"]);
    }

    #[test]
    fn request_precedes_response_for_legacy_timestamp_ties() {
        let timestamp = Utc
            .with_ymd_and_hms(2026, 8, 4, 2, 12, 24)
            .single()
            .expect("timestamp should be valid");
        let mut events = vec![
            usage_event("response", "response", timestamp, json!({})),
            usage_event("request", "request", timestamp, json!({})),
        ];

        UsageEventTimelineOrder::sort(&mut events);

        assert_eq!(event_ids(&events), ["request", "response"]);
    }

    #[test]
    fn broken_cycles_fall_back_to_deterministic_event_identity() {
        let timestamp = Utc
            .with_ymd_and_hms(2026, 8, 4, 2, 12, 24)
            .single()
            .expect("timestamp should be valid");
        let mut events = vec![
            usage_event(
                "second",
                "response",
                timestamp,
                json!({"response_id": "resp_b", "previous_response_id": "resp_a", "provider_call_index": 2_i64}),
            ),
            usage_event(
                "first",
                "response",
                timestamp,
                json!({"response_id": "resp_a", "previous_response_id": "resp_b", "provider_call_index": 1_i64}),
            ),
        ];

        UsageEventTimelineOrder::sort(&mut events);

        assert_eq!(event_ids(&events), ["first", "second"]);
    }

    fn usage_event(
        event_id: &str,
        event_type: &str,
        observed_at: chrono::DateTime<Utc>,
        metadata: serde_json::Value,
    ) -> UsageEvent {
        UsageEvent {
            id: Uuid::nil(),
            user_id: Uuid::nil(),
            device_context_id: Uuid::nil(),
            session_pk: Uuid::nil(),
            turn_pk: Uuid::nil(),
            event_id: event_id.to_owned(),
            agent_name: "codex".to_owned(),
            agent_version: None,
            session_id: "session".to_owned(),
            turn_index: 1,
            llm_provider: "openai".to_owned(),
            llm_model: "gpt-test".to_owned(),
            event_type: event_type.to_owned(),
            text: None,
            text_sha256: None,
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            reasoning_tokens: 0,
            total_tokens: 0,
            observed_at,
            metadata,
            created_at: observed_at,
        }
    }

    fn event_ids(events: &[UsageEvent]) -> Vec<&str> {
        events.iter().map(|event| event.event_id.as_str()).collect()
    }
}
