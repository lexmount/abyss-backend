//! Response tokens per second: an exchange's response output over the time
//! from its request event's observation to its response event's.

use chrono::{DateTime, Utc};

/// Shortest request-to-response time that tells a speed.
const MIN_MICROS: i64 = 100_000;
/// Fastest output speed a response can have; anything faster was not timed.
const MAX_TOKENS_PER_SECOND: i64 = 10_000;

/// An exchange's response output and the microseconds since its request:
/// `None` without a request or a response, without output, or when the
/// timing tells no speed: under 100 ms, or over 10,000 tokens a second.
pub fn timed(
    requested_at: Option<DateTime<Utc>>,
    responded_at: Option<DateTime<Utc>>,
    output: i64,
) -> Option<(i64, i64)> {
    let micros = responded_at?
        .signed_duration_since(requested_at?)
        .num_microseconds()?;
    let too_fast = output.saturating_mul(1_000_000) > MAX_TOKENS_PER_SECOND.saturating_mul(micros);
    (output > 0_i64 && micros >= MIN_MICROS && !too_fast).then_some((output, micros))
}

/// Tokens per second of an output over its microseconds.
#[expect(
    clippy::as_conversions,
    clippy::cast_precision_loss,
    clippy::float_arithmetic,
    reason = "a speed is fractional; token counts and microseconds stay far below 2^53"
)]
pub fn per_second((output, micros): (i64, i64)) -> f64 {
    output as f64 * 1_000_000.0_f64 / micros as f64
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeDelta, Utc};

    use super::{per_second, timed};

    fn after(millis: i64) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
        let requested_at = DateTime::UNIX_EPOCH;
        (
            Some(requested_at),
            requested_at.checked_add_signed(TimeDelta::milliseconds(millis)),
        )
    }

    #[test]
    fn a_response_has_its_output_over_its_time() {
        let (requested_at, responded_at) = after(2_000);

        let speed = timed(requested_at, responded_at, 400).map(per_second);

        assert_eq!(speed, Some(200.0_f64));
    }

    #[test]
    fn a_response_tells_no_speed_without_both_sides_or_output() {
        let (requested_at, responded_at) = after(2_000);

        assert_eq!(timed(None, responded_at, 400), None);
        assert_eq!(timed(requested_at, None, 400), None);
        assert_eq!(timed(requested_at, responded_at, 0), None);
    }

    #[test]
    fn a_timing_too_short_or_too_fast_tells_no_speed() {
        for (millis, output) in [(99, 1), (0, 1), (-5, 1), (1_000, 10_001)] {
            let (requested_at, responded_at) = after(millis);
            assert_eq!(
                timed(requested_at, responded_at, output),
                None,
                "{millis} ms, {output}"
            );
        }
        let (requested_at, responded_at) = after(1_000);
        assert_eq!(
            timed(requested_at, responded_at, 10_000),
            Some((10_000, 1_000_000))
        );
    }
}
