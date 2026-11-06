//! UTC offsets that let SQLite group events by a time zone's calendar days.
//!
//! SQLite has no time zone database, so the offsets a zone applies across the
//! summarized events are worked out here and handed to the query as JSON.

use chrono::{DateTime, Offset, TimeZone, Utc};
use chrono_tz::Tz;

use crate::error::AppError;

const MICROS_PER_SECOND: i64 = 1_000_000;
/// Seconds in one day; no zone changes its offset more than once a day.
const DAY_SECONDS: i64 = 86_400;

/// One UTC offset of a zone and the instant it takes effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct UtcOffsetChange {
    /// Unix second from which the offset applies.
    start_second: i64,
    /// Local time minus UTC, in seconds.
    offset_seconds: i32,
}

/// The offsets `zone` applies to events observed from `first` to `last`
/// (inclusive, in microseconds), as a JSON list of `[start, offset]`
/// microsecond pairs ordered by start. The first pair also covers every
/// earlier instant; `None` means no events, which UTC's single offset covers.
pub(super) fn day_offsets_json(zone: Tz, events: Option<(i64, i64)>) -> Result<String, AppError> {
    let changes = match events {
        Some((first, last)) if zone != Tz::UTC => utc_offset_changes(
            zone,
            first.div_euclid(MICROS_PER_SECOND),
            last.div_euclid(MICROS_PER_SECOND).saturating_add(1),
        ),
        _ => Vec::new(),
    };
    let mut offsets = changes
        .iter()
        .map(|change| {
            [
                change.start_second.saturating_mul(MICROS_PER_SECOND),
                i64::from(change.offset_seconds).saturating_mul(MICROS_PER_SECOND),
            ]
        })
        .collect::<Vec<_>>();
    match offsets.first_mut() {
        Some(first) => first[0] = i64::MIN,
        None => offsets.push([i64::MIN, 0_i64]),
    }
    serde_json::to_string(&offsets)
        .map_err(|error| AppError::internal(format!("serialize day offsets: {error}")))
}

/// The offsets `zone` applies from `from_second` up to `to_second`, in order.
///
/// The range is stepped a day at a time and each change found is narrowed to
/// the exact second it takes effect.
fn utc_offset_changes(zone: Tz, from_second: i64, to_second: i64) -> Vec<UtcOffsetChange> {
    let mut current = offset_at(zone, from_second);
    let mut changes = vec![UtcOffsetChange {
        start_second: from_second,
        offset_seconds: current,
    }];
    let mut cursor = from_second;
    while cursor < to_second {
        let next = cursor.saturating_add(DAY_SECONDS).min(to_second);
        if offset_at(zone, next) == current {
            cursor = next;
            continue;
        }
        // `low` keeps the current offset and `high` does not.
        let (mut low, mut high) = (cursor, next);
        while high.saturating_sub(low) > 1_i64 {
            let middle = low.midpoint(high);
            if offset_at(zone, middle) == current {
                low = middle;
            } else {
                high = middle;
            }
        }
        current = offset_at(zone, high);
        changes.push(UtcOffsetChange {
            start_second: high,
            offset_seconds: current,
        });
        cursor = high;
    }
    changes
}

fn offset_at(zone: Tz, unix_second: i64) -> i32 {
    DateTime::<Utc>::from_timestamp(unix_second, 0).map_or(0_i32, |instant| {
        zone.offset_from_utc_datetime(&instant.naive_utc())
            .fix()
            .local_minus_utc()
    })
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono_tz::Tz;

    use super::{UtcOffsetChange, day_offsets_json, utc_offset_changes};

    fn second(value: &str) -> i64 {
        DateTime::parse_from_rfc3339(value)
            .expect("timestamp should parse")
            .timestamp()
    }

    #[test]
    fn zone_without_changes_keeps_one_offset() {
        assert_eq!(
            utc_offset_changes(
                Tz::Asia__Shanghai,
                second("2026-01-01T00:00:00Z"),
                second("2026-12-31T00:00:00Z"),
            ),
            [UtcOffsetChange {
                start_second: second("2026-01-01T00:00:00Z"),
                offset_seconds: 8 * 3600,
            }]
        );
    }

    #[test]
    fn daylight_saving_changes_start_at_their_exact_second() {
        // Los Angeles moves to PDT at 2026-03-08 02:00 PST and back to PST at
        // 2026-11-01 02:00 PDT.
        assert_eq!(
            utc_offset_changes(
                Tz::America__Los_Angeles,
                second("2026-02-01T00:00:00Z"),
                second("2026-12-01T00:00:00Z"),
            ),
            [
                UtcOffsetChange {
                    start_second: second("2026-02-01T00:00:00Z"),
                    offset_seconds: -8 * 3600,
                },
                UtcOffsetChange {
                    start_second: second("2026-03-08T10:00:00Z"),
                    offset_seconds: -7 * 3600,
                },
                UtcOffsetChange {
                    start_second: second("2026-11-01T09:00:00Z"),
                    offset_seconds: -8 * 3600,
                },
            ]
        );
    }

    #[test]
    fn offsets_cover_every_instant_and_default_to_utc() {
        let minimum = i64::MIN;
        assert_eq!(
            day_offsets_json(Tz::UTC, Some((0, 1))).expect("utc offsets"),
            format!("[[{minimum},0]]")
        );
        assert_eq!(
            day_offsets_json(Tz::Asia__Shanghai, None).expect("no events"),
            format!("[[{minimum},0]]")
        );
        assert_eq!(
            day_offsets_json(Tz::Asia__Shanghai, Some((0, 1))).expect("shanghai offsets"),
            format!("[[{minimum},28800000000]]")
        );
    }
}
