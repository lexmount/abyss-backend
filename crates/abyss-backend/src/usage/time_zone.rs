//! Calendar days in a caller-chosen IANA time zone.
//!
//! Event times are absolute instants. A summary's time zone only decides which
//! local calendar day an instant falls on when the summary groups by day.

use jiff::tz::{TimeZone, TimeZoneDatabase};

use crate::error::AppError;

/// Resolves a summary's `tz` parameter; without one, days follow UTC.
pub fn parse_time_zone(raw: Option<&str>) -> Result<TimeZone, AppError> {
    let Some(name) = raw.map(str::trim).filter(|name| !name.is_empty()) else {
        return Ok(TimeZone::UTC);
    };
    TimeZoneDatabase::bundled()
        .get(name)
        .ok()
        .filter(|zone| !zone.is_unknown() && zone.iana_name() == Some(name))
        .ok_or_else(|| {
            AppError::validation(format!("tz must be an IANA time zone name, got {name:?}"))
        })
}

#[cfg(test)]
mod tests {
    use jiff::tz::TimeZone;

    use super::parse_time_zone;

    #[test]
    fn time_zone_defaults_to_utc_and_rejects_unknown_names() {
        assert_eq!(parse_time_zone(None).expect("absent tz"), TimeZone::UTC);
        assert_eq!(parse_time_zone(Some(" ")).expect("blank tz"), TimeZone::UTC);
        assert_eq!(
            parse_time_zone(Some("Asia/Shanghai"))
                .expect("named tz")
                .iana_name(),
            Some("Asia/Shanghai")
        );
        assert!(parse_time_zone(Some("+08:00")).is_err());
        assert!(parse_time_zone(Some("Etc/Unknown")).is_err());
        assert!(parse_time_zone(Some("Mars/Olympus")).is_err());
    }
}
