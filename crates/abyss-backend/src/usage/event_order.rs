//! Deterministic ordering for normalized events inside Agent timeline turns.
//!
//! Events are ordered by turn, high-precision observation time, event side,
//! and stable event identity.

use chrono::{DateTime, Utc};

/// Event fields required by deterministic timeline ordering.
pub trait TimelineEvent {
    fn turn_index(&self) -> i32;
    fn observed_at(&self) -> DateTime<Utc>;
    fn event_type(&self) -> &str;
    fn event_id(&self) -> &str;
}

#[cfg(feature = "postgres-es")]
impl TimelineEvent for crate::db::models::UsageEvent {
    fn turn_index(&self) -> i32 {
        self.turn_index
    }

    fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }

    fn event_type(&self) -> &str {
        &self.event_type
    }

    fn event_id(&self) -> &str {
        &self.event_id
    }
}

impl TimelineEvent for super::UsageEventOutline {
    fn turn_index(&self) -> i32 {
        self.turn_index
    }

    fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }

    fn event_type(&self) -> &str {
        &self.event_type
    }

    fn event_id(&self) -> &str {
        &self.event_id
    }
}

/// Applies the authoritative event order used by owned and shared timelines.
pub struct UsageEventTimelineOrder;

impl UsageEventTimelineOrder {
    /// Sorts events by turn, observation time, side, and ID.
    ///
    /// The final event identifier tie-breaker makes output deterministic even
    /// for collectors with identical timestamps.
    pub fn sort<T: TimelineEvent>(events: &mut [T]) {
        events.sort_by(|left, right| {
            left.turn_index()
                .cmp(&right.turn_index())
                .then_with(|| left.observed_at().cmp(&right.observed_at()))
                .then_with(|| {
                    event_side_rank(left.event_type()).cmp(&event_side_rank(right.event_type()))
                })
                .then_with(|| left.event_id().cmp(right.event_id()))
        });
    }
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
    use chrono::{DateTime, TimeZone as _, Timelike as _, Utc};

    use super::{TimelineEvent, UsageEventTimelineOrder};

    struct Sample {
        turn_index: i32,
        observed_at: DateTime<Utc>,
        event_type: &'static str,
        event_id: &'static str,
    }

    impl TimelineEvent for Sample {
        fn turn_index(&self) -> i32 {
            self.turn_index
        }

        fn observed_at(&self) -> DateTime<Utc> {
            self.observed_at
        }

        fn event_type(&self) -> &str {
            self.event_type
        }

        fn event_id(&self) -> &str {
            self.event_id
        }
    }

    fn timestamp() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 4, 2, 12, 24)
            .single()
            .expect("timestamp should be valid")
    }

    fn event(
        turn_index: i32,
        observed_at: DateTime<Utc>,
        event_type: &'static str,
        event_id: &'static str,
    ) -> Sample {
        Sample {
            turn_index,
            observed_at,
            event_type,
            event_id,
        }
    }

    fn sorted_ids(mut events: Vec<Sample>) -> Vec<&'static str> {
        UsageEventTimelineOrder::sort(&mut events);
        events.iter().map(|event| event.event_id).collect()
    }

    #[test]
    fn turn_precedes_observation_time() {
        let first = timestamp();
        let second = first + chrono::TimeDelta::microseconds(1);
        assert_eq!(
            sorted_ids(vec![
                event(2, first, "request", "turn-2"),
                event(1, second, "request", "turn-1"),
            ]),
            ["turn-1", "turn-2"]
        );
    }

    #[test]
    fn high_precision_timestamp_orders_events_within_a_turn() {
        let first = timestamp()
            .with_nanosecond(1_000)
            .expect("microsecond should be valid");
        let second = first
            .with_nanosecond(2_000)
            .expect("microsecond should be valid");
        assert_eq!(
            sorted_ids(vec![
                event(1, second, "request", "second"),
                event(1, first, "response", "first"),
            ]),
            ["first", "second"]
        );
    }

    #[test]
    fn request_precedes_response_for_timestamp_ties() {
        assert_eq!(
            sorted_ids(vec![
                event(1, timestamp(), "response", "response"),
                event(1, timestamp(), "request", "request"),
            ]),
            ["request", "response"]
        );
    }

    #[test]
    fn event_identity_breaks_remaining_ties() {
        assert_eq!(
            sorted_ids(vec![
                event(1, timestamp(), "response", "b"),
                event(1, timestamp(), "response", "a"),
            ]),
            ["a", "b"]
        );
    }
}
