//! Tests of image bytes stored once per user and digest in SQLite.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use diesel::{
    Connection, QueryableByName, RunQueryDsl, SqliteConnection, connection::SimpleConnection,
    sql_query, sql_types::Text,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    migrations,
    repository::{image_attachment, ingest_events},
};

#[derive(QueryableByName)]
struct Row {
    #[diesel(sql_type = Text)]
    id: String,
}

fn ingest(database: &mut SqliteConnection, user: Uuid, event_id: &str, image: &[u8], bytes: bool) {
    let mut attachment = json!({
        "position": 0_i32,
        "media_type": "image/png",
        "byte_size": image.len(),
        "sha256": hex::encode(Sha256::digest(image)),
    });
    if bytes {
        attachment["content_base64"] = json!(STANDARD.encode(image));
    }
    let request = serde_json::from_value(json!({"events": [{
        "event_id": event_id,
        "observed_at": "2026-11-12T00:00:00Z",
        "device": {"host_name": "local", "platform": "macos"},
        "agent": {"name": "claude-code"},
        "session_id": "images",
        "llm": {"provider": "anthropic", "model": "claude"},
        "event_type": "request",
        "token_usage": {"input_tokens": 1_i64, "total_tokens": 1_i64},
        "attachments": [attachment]
    }]}))
    .expect("a test ingest request");
    ingest_events(database, &request, user, 100).expect("ingest should succeed");
}

/// The ids of a user's attachments, in insertion order.
fn attachment_ids(database: &mut SqliteConnection, user: Uuid) -> Vec<Uuid> {
    sql_query("SELECT id FROM llm_usage_event_attachments WHERE user_id = ? ORDER BY id")
        .bind::<Text, _>(user.to_string())
        .load::<Row>(database)
        .expect("attachments load")
        .into_iter()
        .map(|row| Uuid::parse_str(&row.id).expect("a UUID"))
        .collect()
}

#[test]
fn a_users_repeated_image_is_stored_once_and_never_shared() {
    let owner = Uuid::from_u128(1);
    let other = Uuid::from_u128(2);
    let mut database = SqliteConnection::establish(":memory:").expect("SQLite should open");
    database
        .batch_execute("PRAGMA foreign_keys = ON;")
        .expect("foreign keys are enforced");
    migrations::run(&mut database).expect("migration should succeed");
    database
        .batch_execute(&format!(
            "INSERT INTO app_users (id, email, created_at) VALUES ('{other}', 'other@abyss.local', 0)"
        ))
        .expect("a second user");
    let image = b"\x89PNG\r\n\x1a\nimage".to_vec();

    ingest(&mut database, owner, "first", &image, true);
    ingest(&mut database, owner, "second", &image, true);
    // Another user names the same digest without its bytes.
    ingest(&mut database, other, "other", &image, false);

    let blobs = sql_query("SELECT hex(user_id) AS id FROM attachment_blobs")
        .load::<Row>(&mut database)
        .expect("blobs load");
    assert_eq!(blobs.len(), 2);
    for id in attachment_ids(&mut database, owner) {
        let stored = image_attachment(&mut database, owner, id).expect("the owner's image");
        assert_eq!(stored.content, image);
    }
    let [id] = attachment_ids(&mut database, other)[..] else {
        panic!("one attachment of the other user");
    };
    assert!(image_attachment(&mut database, other, id).is_err());
}
