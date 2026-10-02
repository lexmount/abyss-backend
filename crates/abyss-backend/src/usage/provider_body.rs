//! Stateless decoding of archived provider content, shared by projections.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;

#[derive(Clone)]
pub struct Message {
    pub id: Option<String>,
    pub role: String,
    pub text: String,
}

pub fn request_payload(values: &[Value]) -> Option<&Value> {
    values
        .iter()
        .map(|v| v.get("response").unwrap_or(v))
        .find(|v| v.get("input").is_some() || v.get("messages").is_some())
}

/// Decode bytes only for projection. The original base64, whitespace, unknown
/// fields, and SSE events remain untouched in event metadata.
pub fn bodies(metadata: &Value) -> Option<Vec<Value>> {
    let parts = metadata.get("raw_body")?.get("parts")?.as_array()?;
    let mut values = Vec::new();
    for part in parts {
        if part.get("truncated").and_then(Value::as_bool) != Some(false)
            || part
                .get("content_encoding")
                .and_then(Value::as_str)
                .is_some_and(|v| v != "identity")
        {
            return None;
        }
        let bytes = STANDARD
            .decode(part.get("content_base64")?.as_str()?)
            .ok()?;
        if let Ok(value) = serde_json::from_slice(&bytes) {
            values.push(value);
        } else {
            let text = std::str::from_utf8(&bytes).ok()?;
            let mut data = String::new();
            for line in text.lines().chain(std::iter::once("")) {
                if line.is_empty() {
                    if let Ok(value) = serde_json::from_str(&data) {
                        values.push(value);
                    }
                    data.clear();
                } else if let Some(line) = line.strip_prefix("data:") {
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(line.strip_prefix(' ').unwrap_or(line));
                }
            }
        }
    }
    Some(values)
}

pub fn request_messages(payload: &Value) -> Vec<Message> {
    let mut result = Vec::new();
    for (key, role) in [("instructions", "developer"), ("system", "system")] {
        if let Some(value) = payload.get(key) {
            result.push(Message {
                id: None,
                role: role.to_owned(),
                text: text(value),
            });
        }
    }
    if let Some(input) = payload.get("input").or_else(|| payload.get("messages")) {
        if let Some(items) = input.as_array() {
            result.extend(items.iter().filter_map(|v| message(v, "request")));
        } else {
            result.extend(message(input, "request"));
        }
    }
    result
}

pub fn message(value: &Value, default_role: &str) -> Option<Message> {
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let role = value
        .get("role")
        .and_then(Value::as_str)
        .unwrap_or(default_role);
    if role == "tool" || tool(kind) {
        return None;
    }
    let (role, content) = if kind == "reasoning" {
        ("reasoning", value.get("summary")?)
    } else {
        (role, value.get("content").unwrap_or(value))
    };
    Some(Message {
        id: value.get("id").and_then(Value::as_str).map(str::to_owned),
        role: role.to_owned(),
        text: text(content),
    })
}

pub fn text(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(text)
            .filter(|v| !v.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(obj) => {
            let kind = obj.get("type").and_then(Value::as_str).unwrap_or_default();
            if tool(kind) {
                return String::new();
            }
            obj.get("text")
                .or_else(|| obj.get("content"))
                .or_else(|| obj.get("thinking"))
                .or_else(|| obj.get("output_text"))
                .map(text)
                .unwrap_or_default()
        }
        _ => String::new(),
    }
}

fn tool(kind: &str) -> bool {
    matches!(kind, "tool_use" | "tool_result" | "tool_call")
        || kind.ends_with("_call")
        || kind.ends_with("_call_output")
}
