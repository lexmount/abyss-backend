//! JSON event encoding with explicit workload identity and flat request context.

use std::fmt;

use serde_json::{Map, Value};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    fmt::{
        FmtContext, FormatEvent, FormattedFields,
        format::{JsonFields, Writer},
    },
    registry::LookupSpan,
};

pub struct JsonFormat {
    service: &'static str,
    environment: String,
}

impl JsonFormat {
    pub(super) fn new(service: &'static str, environment: &str) -> Self {
        Self {
            service,
            environment: environment.to_owned(),
        }
    }
}

impl<S> FormatEvent<S, JsonFields> for JsonFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, JsonFields>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut fields = Fields(Map::new());
        event.record(&mut fields);
        if let Some(scope) = context.event_scope() {
            for span in scope.from_root() {
                let extensions = span.extensions();
                if let Some(json) = extensions.get::<FormattedFields<JsonFields>>() {
                    let values: Map<String, Value> =
                        serde_json::from_str(json).map_err(|_| fmt::Error)?;
                    // Only the middleware owns these keys. Do not promote arbitrary
                    // span fields into the operational query contract.
                    for key in ["request_id", "method", "route"] {
                        if let Some(value) = values.get(key) {
                            fields.0.insert(key.to_owned(), value.clone());
                        }
                    }
                }
            }
        }
        fields.0.insert(
            "timestamp".to_owned(),
            chrono::Utc::now().to_rfc3339().into(),
        );
        fields.0.insert(
            "level".to_owned(),
            event.metadata().level().to_string().into(),
        );
        fields
            .0
            .insert("target".to_owned(), event.metadata().target().into());
        fields.0.insert("service".to_owned(), self.service.into());
        fields
            .0
            .insert("environment".to_owned(), self.environment.clone().into());
        // The immutable image identifier supplied by Kubernetes identifies the
        // source revision; this field identifies the built package version.
        fields
            .0
            .insert("version".to_owned(), env!("CARGO_PKG_VERSION").into());
        writeln!(writer, "{}", Value::Object(fields.0))
    }
}

struct Fields(Map<String, Value>);

impl Visit for Fields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .insert(field.name().to_owned(), format!("{value:?}").into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.into());
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_owned(), value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().to_owned(), value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().to_owned(), value.into());
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().to_owned(), value.into());
    }
}
