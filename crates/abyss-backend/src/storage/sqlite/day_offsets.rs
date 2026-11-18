//! UTC offsets that let SQLite group events by a time zone's calendar days.
//!
//! SQLite has no time zone database, so the offsets a zone applies across the
//! summarized events are worked out here and handed to the query as JSON.

use jiff::Timestamp;
use jiff::tz::TimeZone;

use crate::error::AppError;

const MICROS_PER_SECOND: i64 = 1_000_000;

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
pub(super) fn day_offsets_json(
    zone: &TimeZone,
    events: Option<(i64, i64)>,
) -> Result<String, AppError> {
    let changes = match events {
        Some((first, last)) if zone != &TimeZone::UTC => utc_offset_changes(
            zone,
            Timestamp::from_microsecond(first)
                .map_err(|error| AppError::internal(error.to_string()))?,
            Timestamp::from_microsecond(last)
                .map_err(|error| AppError::internal(error.to_string()))?,
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

/// Includes the offset at the first event and every transition through the last event.
fn utc_offset_changes(zone: &TimeZone, first: Timestamp, last: Timestamp) -> Vec<UtcOffsetChange> {
    let initial = UtcOffsetChange {
        start_second: first.as_second(),
        offset_seconds: zone.to_offset(first).seconds(),
    };
    std::iter::once(initial)
        .chain(
            zone.following(first)
                .take_while(|change| change.timestamp() <= last)
                .map(|change| UtcOffsetChange {
                    start_second: change.timestamp().as_second(),
                    offset_seconds: change.offset().seconds(),
                }),
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use jiff::Timestamp;
    use jiff::tz::TimeZone;

    use super::{UtcOffsetChange, day_offsets_json, utc_offset_changes};

    fn instant(value: &str) -> Timestamp {
        value.parse::<Timestamp>().expect("timestamp should parse")
    }

    #[test]
    fn zone_without_changes_keeps_one_offset() {
        assert_eq!(
            utc_offset_changes(
                &TimeZone::get("Asia/Shanghai").expect("Shanghai zone"),
                instant("2026-01-01T00:00:00Z"),
                instant("2026-12-31T00:00:00Z"),
            ),
            [UtcOffsetChange {
                start_second: instant("2026-01-01T00:00:00Z").as_second(),
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
                &TimeZone::get("America/Los_Angeles").expect("Los Angeles zone"),
                instant("2026-02-01T00:00:00Z"),
                instant("2026-12-01T00:00:00Z"),
            ),
            [
                UtcOffsetChange {
                    start_second: instant("2026-02-01T00:00:00Z").as_second(),
                    offset_seconds: -8 * 3600,
                },
                UtcOffsetChange {
                    start_second: instant("2026-03-08T10:00:00Z").as_second(),
                    offset_seconds: -7 * 3600,
                },
                UtcOffsetChange {
                    start_second: instant("2026-11-01T09:00:00Z").as_second(),
                    offset_seconds: -8 * 3600,
                },
            ]
        );
    }

    #[test]
    fn offsets_cover_every_instant_and_default_to_utc() {
        let minimum = i64::MIN;
        assert_eq!(
            day_offsets_json(&TimeZone::UTC, Some((0, 1))).expect("utc offsets"),
            format!("[[{minimum},0]]")
        );
        assert_eq!(
            day_offsets_json(
                &TimeZone::get("Asia/Shanghai").expect("Shanghai zone"),
                None
            )
            .expect("no events"),
            format!("[[{minimum},0]]")
        );
        assert_eq!(
            day_offsets_json(
                &TimeZone::get("Asia/Shanghai").expect("Shanghai zone"),
                Some((0, 1))
            )
            .expect("shanghai offsets"),
            format!("[[{minimum},28800000000]]")
        );
    }
}
