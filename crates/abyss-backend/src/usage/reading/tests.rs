//! Regression cases for lossless cursor traversal and recorded-scope integrity.
use super::*;
use serde_json::json;

fn index(count: u128) -> ReadingIndex {
    ReadingIndex::new(
        Uuid::from_u128(99),
        (1..=count)
            .map(|id| SourceEvent {
                event_pk: Uuid::from_u128(id),
                event_id: format!("event-{id}"),
                observed_at: DateTime::from_timestamp(0, 0).expect("valid timestamp"),
                event_type: "request".to_owned(),
            })
            .collect(),
    )
    .expect("nonempty index")
}

fn read(query: &ReadQuery, value: &Value) -> ReadPage {
    let index = index(2);
    let position = index.position(query).expect("valid query");
    index.read(position, value).expect("valid read")
}

#[test]
fn lossless_unicode_chunks_resume_across_events_and_report_capture_gaps() {
    let event = json!({"conversation":{"completeness":"partial","entries":[{"role":"tool_result","text":"a🦀é\n\"\\".repeat(200)}]},"omitted":["images"],"attachments":[{"id":"image"}]});
    let expected = serde_json::to_string(&event).expect("JSON");
    let mut query = ReadQuery {
        max_bytes: Some(7),
        ..ReadQuery::default()
    };
    let mut reconstructed = String::new();
    let mut finished = 0_usize;
    loop {
        let page = read(&query, &event);
        assert!(page.content_json.len() <= 7, "chunk obeys byte budget");
        assert_eq!(page.byte_offset, reconstructed.len());
        assert_eq!(page.capture.completeness.as_deref(), Some("partial"));
        assert_eq!(page.capture.omitted, json!(["images"]));
        assert_eq!(page.capture.attachment_count, 1);
        reconstructed.push_str(&page.content_json);
        if page.event_complete {
            assert_eq!(reconstructed, expected);
            reconstructed.clear();
            finished += 1;
        }
        query.cursor = page.next_cursor;
        if page.session_complete {
            break;
        }
        assert!(query.cursor.is_some(), "unfinished scope has a cursor");
    }
    assert_eq!(finished, 2);
    assert!(query.cursor.is_none());
}

#[test]
fn changed_membership_or_content_rejects_resume() {
    let page = read(
        &ReadQuery {
            max_bytes: Some(4),
            ..ReadQuery::default()
        },
        &json!({"text":"original"}),
    );
    let query = ReadQuery {
        cursor: page.next_cursor,
        ..ReadQuery::default()
    };
    assert!(
        index(3).position(&query).is_err(),
        "late event invalidates directory"
    );
    let index = index(2);
    let position = index.position(&query).expect("same directory");
    assert!(index.read(position, &json!({"text":"changed"})).is_err());
}

#[test]
fn directory_pages_are_bounded_and_revision_checked() {
    let page = index(101)
        .directory(&DirectoryQuery::default())
        .expect("directory");
    assert_eq!(page.events.len(), 50);
    assert_eq!(page.next_offset, Some(50));
    let final_page = index(101)
        .directory(&DirectoryQuery {
            offset: Some(100),
            revision: Some(page.revision),
            ..DirectoryQuery::default()
        })
        .expect("final directory");
    assert_eq!(final_page.events.len(), 1);
    assert_eq!(final_page.next_offset, None);
    for query in [
        DirectoryQuery {
            limit: Some(0),
            ..DirectoryQuery::default()
        },
        DirectoryQuery {
            offset: Some(102),
            ..DirectoryQuery::default()
        },
        DirectoryQuery {
            revision: Some("stale".to_owned()),
            ..DirectoryQuery::default()
        },
    ] {
        assert!(index(101).directory(&query).is_err());
    }
}

#[test]
fn invalid_positions_and_cursor_binding_fail_closed() {
    for query in [
        ReadQuery {
            max_bytes: Some(3),
            ..ReadQuery::default()
        },
        ReadQuery {
            max_bytes: Some(MAX_CHUNK_BYTES + 1),
            ..ReadQuery::default()
        },
        ReadQuery {
            event_index: Some(2),
            ..ReadQuery::default()
        },
        ReadQuery {
            cursor: Some("invalid".to_owned()),
            ..ReadQuery::default()
        },
    ] {
        assert!(index(2).position(&query).is_err());
    }
    let page = read(
        &ReadQuery {
            max_bytes: Some(4),
            ..ReadQuery::default()
        },
        &json!({"text":"🦀"}),
    );
    let encoded = page.next_cursor.expect("cursor");
    let mut cursor: Cursor =
        serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded).expect("base64"))
            .expect("cursor JSON");
    cursor.session_pk = Uuid::nil();
    let query = ReadQuery {
        cursor: Some(URL_SAFE_NO_PAD.encode(serde_json::to_vec(&cursor).expect("JSON"))),
        ..ReadQuery::default()
    };
    assert!(index(2).position(&query).is_err());
}

#[test]
fn empty_store_is_not_found_and_absent_conversation_is_explicit() {
    assert!(ReadingIndex::new(Uuid::nil(), vec![]).is_err());
    let page = read(&ReadQuery::default(), &json!({"omitted":["messages"]}));
    assert!(!page.capture.conversation_present);
    assert_eq!(page.capture.completeness, None);
    assert_eq!(page.capture.scope, "recorded_events_only");
}
