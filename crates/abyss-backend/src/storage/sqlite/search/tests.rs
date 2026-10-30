//! SQLite search against stored events, and against a naive grep of every
//! piece of every event.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use diesel::{
    Connection, ExpressionMethods, QueryDsl, RunQueryDsl, SqliteConnection,
    connection::SimpleConnection, dsl::count_star,
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{
    search::{
        InSessionSearchQuery, InSessionSearchResponse, SessionSearchQuery, SessionSearchResponse,
    },
    usage::{IngestEventsRequest, segments::SegmentQuery},
};

use crate::storage::stream_cache::StreamCache;

use super::{
    super::{
        migrations,
        repository::{ingest_events, session_segment, session_streams},
        schema::{agent_sessions, search_refs, search_texts},
    },
    search_in_session, session_search,
};

const OWNER_ID: Uuid = Uuid::from_u128(1);
const OTHER_OWNER_ID: Uuid = Uuid::from_u128(2);

/// One event; a request `x` and its response `xr` share a response id, so
/// they form one exchange.
fn event(id: &str, session: &str, second: u32, event_type: &str, entries: &Value) -> Value {
    json!({
        "event_id": id,
        "observed_at": format!("2026-09-01T00:00:{second:02}Z"),
        "device": {"host_name": "alice-mbp", "platform": "macos"},
        "agent": {"name": "codex_exec", "version": "0.9"},
        "session_id": session,
        "response_id": format!("resp-{}", id.strip_suffix('r').unwrap_or(id)),
        "llm": {"provider": "openai", "model": "gpt-5"},
        "event_type": event_type,
        "token_usage": {"input_tokens": 1_i64, "output_tokens": 1_i64},
        "conversation": {"entries": entries, "completeness": "complete"},
        "metadata": {
            "request_headers": [{"name": "x-api-key", "value": "test-secret"}]
        }
    })
}

fn database() -> SqliteConnection {
    let mut database = SqliteConnection::establish(":memory:").expect("SQLite should open");
    migrations::run(&mut database).expect("migration should succeed");
    database
        .batch_execute(&format!(
            "INSERT INTO app_users (id, email, created_at) \
             VALUES ('{OTHER_OWNER_ID}', 'other@abyss.local', 0)"
        ))
        .expect("a second owner is created");
    database
}

fn ingest(database: &mut SqliteConnection, owner: Uuid, events: &[Value]) {
    let request: IngestEventsRequest =
        serde_json::from_value(json!({ "events": events })).expect("request should parse");
    ingest_events(database, &request, owner, 1_000).expect("ingest should succeed");
}

fn search(database: &mut SqliteConnection, query: Value) -> SessionSearchResponse {
    session_search(
        database,
        OWNER_ID,
        serde_json::from_value::<SessionSearchQuery>(query).expect("query should deserialize"),
    )
    .expect("search should succeed")
}

/// The key of the owner's session with the declared id.
fn session_pk(database: &mut SqliteConnection, owner: Uuid, session: &str) -> Uuid {
    let key = agent_sessions::table
        .filter(agent_sessions::user_id.eq(owner.to_string()))
        .filter(agent_sessions::session_id.eq(session))
        .select(agent_sessions::id)
        .first::<String>(database)
        .expect("the session is stored");
    Uuid::parse_str(&key).expect("a session key is a UUID")
}

fn search_session(
    database: &mut SqliteConnection,
    session: &str,
    q: &str,
) -> InSessionSearchResponse {
    let session_pk = session_pk(database, OWNER_ID, session);
    search_in_session(
        database,
        OWNER_ID,
        session_pk,
        &InSessionSearchQuery { q: q.to_owned() },
        &StreamCache::default(),
    )
    .expect("search should succeed")
}

/// The head of the segment holding each event of a session, read through
/// its streams and segments as the session page reads them.
fn segment_heads(database: &mut SqliteConnection, session: &str) -> HashMap<Uuid, String> {
    let session_pk = session_pk(database, OWNER_ID, session);
    let streams = session_streams(database, OWNER_ID, session_pk, &StreamCache::default())
        .expect("the session has streams");
    let query = SegmentQuery::default().validate().expect("a valid query");
    let mut heads = HashMap::new();
    for segment in streams.segments {
        let run = session_segment(
            database,
            OWNER_ID,
            session_pk,
            &segment.head,
            &query,
            &StreamCache::default(),
        )
        .expect("the segment exists");
        for exchange in run.exchanges {
            for pk in [exchange.exchange.request_pk, exchange.exchange.response_pk]
                .into_iter()
                .flatten()
            {
                heads.insert(pk, segment.head.clone());
            }
        }
    }
    heads
}

fn user(text: &str) -> Value {
    json!({"role": "user", "text": text})
}

fn assistant(text: &str) -> Value {
    json!({"role": "assistant", "text": text})
}

fn call(id: &str, name: &str, text: &str) -> Value {
    json!({"role": "tool_call", "text": text, "call_id": id, "name": name})
}

fn result(id: &str, text: &str) -> Value {
    json!({"role": "tool_result", "text": text, "call_id": id})
}

/// Requests that replay their history, responses, and another owner's copy
/// of the same text.
fn fixture() -> Vec<Value> {
    let system = json!({"role": "system", "text": "You are Abyss. 先搜索，再回答。Use NOTES.md."});
    let ask = user("find nimbus.example.org in NOTES.md");
    let grep = call("c1", "Grep", "{\"pattern\":\"nimbus\"}");
    let found = result("c1", "NOTES.md: nimbus.example.org 😀 NIMBUS.EXAMPLE.ORG");
    let answer = assistant("开始新的任务 for the branch; run cargo test.");
    let mut events = vec![
        event("a1", "alpha", 1, "request", &json!([system, ask])),
        event("a1r", "alpha", 2, "response", &json!([grep])),
        event(
            "a2",
            "alpha",
            3,
            "request",
            &json!([system, ask, grep, found]),
        ),
        event("a2r", "alpha", 4, "response", &json!([answer])),
        event(
            "a3",
            "alpha",
            5,
            "request",
            &json!([
                system,
                ask,
                grep,
                found,
                answer,
                user("cargo test 搜索 again")
            ]),
        ),
        event("a3r", "alpha", 6, "response", &json!([assistant("ok")])),
        event(
            "b1",
            "beta",
            7,
            "request",
            &json!([
                json!({"role": "developer", "text": "ÄBC ΣΟΦΙΑ Straße İstanbul ＡＢＣ ﬁle"}),
                user("ababa aaaaa \u{212a}elvin a\0b\0c abc"),
            ]),
        ),
        event(
            "b1r",
            "beta",
            7,
            "response",
            &json!([call("c2", "Read", "NOTES.md")]),
        ),
        event(
            "b2",
            "beta",
            8,
            "request",
            &json!([
                json!({"role": "developer", "text": "ÄBC ΣΟΦΙΑ Straße İstanbul ＡＢＣ ﬁle"}),
                user("ababa aaaaa \u{212a}elvin a\0b\0c abc"),
                call("c2", "Read", "NOTES.md"),
                result("c2", "# Notes\nbranches/search 开始新的任务"),
                json!({"role": "reasoning", "text": ""}),
            ]),
        ),
        event(
            "g1",
            "gamma",
            9,
            "response",
            &json!([assistant("搜索 cargo test nimbus")]),
        ),
    ];
    events.push(event("e1", "empty", 11, "request", &json!([])));
    events
}

fn seeded() -> SqliteConnection {
    let mut database = database();
    ingest(&mut database, OWNER_ID, &fixture());
    let other = fixture()
        .into_iter()
        .map(|mut event| {
            event["event_id"] = json!(format!(
                "other-{}",
                event["event_id"].as_str().unwrap_or_default()
            ));
            event["session_id"] = json!("other-session");
            event
        })
        .collect::<Vec<_>>();
    ingest(&mut database, OTHER_OWNER_ID, &other);
    database
}

fn count(value: usize) -> u64 {
    u64::try_from(value).expect("a count fits")
}

#[derive(Clone, Copy)]
enum Table {
    Texts,
    Refs,
}

fn stored(database: &mut SqliteConnection, table: Table) -> u64 {
    let rows = match table {
        Table::Texts => search_texts::table
            .select(count_star())
            .first::<i64>(database),
        Table::Refs => search_refs::table
            .select(count_star())
            .first::<i64>(database),
    };
    u64::try_from(rows.expect("rows count")).expect("a count fits")
}

/// Every stored event of the owner as the naive grep reads it.
struct StoredEvent {
    pk: Uuid,
    event_id: String,
    session_id: String,
    observed_at: i64,
    response: bool,
    entries: Vec<Value>,
}

fn stored_events(database: &mut SqliteConnection, owner: Uuid) -> Vec<StoredEvent> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        event_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        session_id: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        observed_at: i64,
        #[diesel(sql_type = diesel::sql_types::Text)]
        event_type: String,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        conversation: Option<String>,
    }
    diesel::sql_query(
        "SELECT id, event_id, session_id, observed_at, event_type, conversation \
         FROM llm_usage_events WHERE user_id = ?",
    )
    .bind::<diesel::sql_types::Text, _>(owner.to_string())
    .load::<Row>(database)
    .expect("events should load")
    .into_iter()
    .map(|row| {
        let conversation = row.conversation.map_or(Value::Null, |conversation| {
            serde_json::from_str::<Value>(&conversation).expect("conversation is JSON")
        });
        StoredEvent {
            pk: Uuid::parse_str(&row.id).expect("event id is a UUID"),
            event_id: row.event_id,
            session_id: row.session_id,
            observed_at: row.observed_at,
            response: row.event_type == "response",
            entries: conversation["entries"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
        }
    })
    .collect()
}

/// The reference fold, written without the shared implementation.
fn naive_fold(text: &str) -> Vec<char> {
    text.chars()
        .map(|character| {
            let lower = character.to_lowercase().collect::<Vec<_>>();
            match lower.as_slice() {
                [folded] => *folded,
                _ => character,
            }
        })
        .collect()
}

/// Non-overlapping occurrences as UTF-16 ranges, by sliding over characters.
fn naive_ranges(text: &str, query: &str) -> Vec<[usize; 2]> {
    let haystack = naive_fold(text);
    let needle = naive_fold(query.trim());
    let originals = text.chars().collect::<Vec<_>>();
    let utf16 = |end: usize| {
        originals[..end]
            .iter()
            .map(|character| character.len_utf16())
            .sum::<usize>()
    };
    let mut ranges = Vec::new();
    let mut start = 0_usize;
    while let Some(window) = haystack.get(start..start.saturating_add(needle.len())) {
        if window.len() < needle.len() {
            break;
        }
        if window == needle.as_slice() {
            let end = start.saturating_add(needle.len());
            ranges.push([utf16(start), utf16(end)]);
            start = end;
        } else {
            start = start.saturating_add(1);
        }
    }
    ranges
}

/// A request's new part starts after its last model-side entry.
fn naive_new_from(entries: &[Value]) -> usize {
    entries
        .iter()
        .rposition(|entry| {
            matches!(
                entry["role"].as_str(),
                Some("assistant" | "reasoning" | "tool_call")
            )
        })
        .map_or(0, |last| last.saturating_add(1))
}

/// (event, entry, field) → (new, ranges) for every piece holding the query.
type NaiveHits = BTreeMap<(Uuid, usize, &'static str), (bool, Vec<[usize; 2]>)>;

fn naive_hits(events: &[StoredEvent], query: &str, session: Option<&str>) -> NaiveHits {
    let mut hits = BTreeMap::new();
    for event in events {
        if session.is_some_and(|session| session != event.session_id) {
            continue;
        }
        let new_from = naive_new_from(&event.entries);
        for (index, entry) in event.entries.iter().enumerate() {
            for field in ["name", "text"] {
                let Some(text) = entry[field].as_str().filter(|text| !text.is_empty()) else {
                    continue;
                };
                let ranges = naive_ranges(text, query);
                if !ranges.is_empty() {
                    hits.insert(
                        (event.pk, index, field),
                        (event.response || index >= new_from, ranges),
                    );
                }
            }
        }
    }
    hits
}

const QUERIES: [&str; 22] = [
    "搜索",
    "nimbus.example.org",
    "NIMBUS",
    "branch",
    "开始新的任务",
    "cargo test",
    "NOTES.md",
    "a",
    "s",
    "ab",
    "aa",
    "😀",
    "äbc",
    "σοφ",
    "grep",
    "\"pattern\"",
    "ss",
    "ß",
    "i̇",
    "İ",
    "kelvin",
    "bc",
];

#[test]
fn session_search_returns_the_naive_greps_hit_events() {
    let mut database = seeded();
    let events = stored_events(&mut database, OWNER_ID);
    let times = events
        .iter()
        .map(|event| (event.pk, event.observed_at))
        .collect::<BTreeMap<_, _>>();
    for query in QUERIES.into_iter().chain(["nothing-matches-this", "a\0b"]) {
        let naive = naive_hits(&events, query, None);
        // Per session: hit events, and those with a new hit.
        let mut expected = BTreeMap::<String, (BTreeSet<Uuid>, BTreeSet<Uuid>)>::new();
        for ((pk, _, _), (new, _)) in &naive {
            let session = events
                .iter()
                .find(|event| event.pk == *pk)
                .map(|event| event.session_id.clone())
                .expect("hits come from stored events");
            let entry = expected.entry(session).or_default();
            entry.0.insert(*pk);
            if *new {
                entry.1.insert(*pk);
            }
        }
        let mut order = expected
            .iter()
            .map(|(session, (hit_events, _))| {
                let latest = hit_events
                    .iter()
                    .map(|pk| times[pk])
                    .max()
                    .expect("a session has hits");
                (std::cmp::Reverse(latest), session.clone())
            })
            .collect::<Vec<_>>();
        order.sort();

        let mut found = Vec::new();
        for page in 1_u32.. {
            let response = search(
                &mut database,
                json!({"q": query, "page": page, "page_size": 2_u32}),
            );
            assert_eq!(response.total_sessions, count(expected.len()), "{query:?}");
            let done = !response.has_more;
            found.extend(response.items);
            if done {
                break;
            }
        }
        assert_eq!(
            found
                .iter()
                .map(|item| item.session_id.clone())
                .collect::<Vec<_>>(),
            order
                .iter()
                .map(|(_, session)| session.clone())
                .collect::<Vec<_>>(),
            "sessions order by their latest hit event for {query:?}"
        );
        for item in &found {
            let (hit_events, new_events) = &expected[&item.session_id];
            assert_eq!(item.match_count, count(hit_events.len()), "{query:?}");
            assert_eq!(item.new_match_count, count(new_events.len()), "{query:?}");
            let mut by_time = hit_events.iter().copied().collect::<Vec<_>>();
            by_time.sort_by_key(|pk| (times[pk], *pk));
            let shown = by_time
                .iter()
                .filter(|pk| new_events.contains(*pk))
                .chain(by_time.iter().filter(|pk| !new_events.contains(*pk)))
                .take(3)
                .map(|pk| (*pk, new_events.contains(pk)))
                .collect::<Vec<_>>();
            assert_eq!(
                item.matches
                    .iter()
                    .map(|matched| (matched.event_pk, matched.new))
                    .collect::<Vec<_>>(),
                shown,
                "shown matches for {query:?}"
            );
            for matched in &item.matches {
                assert!(
                    !matched.fragments.is_empty() && matched.fragments.len() <= 2,
                    "{query:?}"
                );
                for fragment in &matched.fragments {
                    let highlighted = fragment
                        .segments
                        .iter()
                        .filter(|segment| segment.highlighted)
                        .map(|segment| naive_fold(&segment.text))
                        .collect::<Vec<_>>();
                    assert!(!highlighted.is_empty(), "{query:?}");
                    assert!(
                        highlighted.iter().all(|text| *text == naive_fold(query)),
                        "fragments highlight occurrences of {query:?}: {fragment:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn search_in_session_returns_the_naive_greps_hits_and_ranges() {
    let mut database = seeded();
    let events = stored_events(&mut database, OWNER_ID);
    let times = events
        .iter()
        .map(|event| (event.pk, event.observed_at))
        .collect::<BTreeMap<_, _>>();
    for session in ["alpha", "beta", "gamma", "empty"] {
        let heads = segment_heads(&mut database, session);
        for query in QUERIES.into_iter().chain(["nothing-matches-this"]) {
            let naive = naive_hits(&events, query, Some(session));
            let response = search_session(&mut database, session, query);
            assert!(!response.truncated);
            for hit in &response.hits {
                assert_eq!(
                    hit.segment.as_ref(),
                    heads.get(&hit.event_pk),
                    "a hit names the segment the session page puts its event in"
                );
            }
            let ranges = response
                .texts
                .iter()
                .map(|text| (text.id, text.ranges.clone()))
                .collect::<BTreeMap<_, _>>();
            let got = response
                .hits
                .iter()
                .map(|hit| {
                    (
                        (
                            hit.event_pk,
                            usize::try_from(hit.entry_index).expect("an index fits"),
                            hit.field.as_str(),
                        ),
                        (hit.new, ranges[&hit.text_id].clone()),
                    )
                })
                .collect::<NaiveHits>();
            assert_eq!(got, naive, "{session} {query:?}");
            assert_eq!(got.len(), response.hits.len(), "a piece is one hit");
            assert_eq!(
                ranges.len(),
                response
                    .hits
                    .iter()
                    .map(|hit| hit.text_id)
                    .collect::<BTreeSet<_>>()
                    .len(),
                "texts are those the hits name"
            );
            let mut ordered = response.hits.clone();
            ordered.sort_by_key(|hit| {
                (
                    times[&hit.event_pk],
                    hit.event_pk,
                    hit.entry_index,
                    hit.field,
                )
            });
            assert_eq!(
                ordered, response.hits,
                "hits are in session order for {query:?}"
            );
        }
    }
}

#[test]
fn ingest_stores_each_distinct_text_once_per_owner() {
    let mut database = database();
    ingest(&mut database, OWNER_ID, &fixture());
    let texts = stored(&mut database, Table::Texts);
    let refs = stored(&mut database, Table::Refs);
    let events = stored_events(&mut database, OWNER_ID);
    let pieces = events
        .iter()
        .flat_map(|event| event.entries.iter())
        .flat_map(|entry| {
            ["name", "text"].map(|field| {
                entry[field]
                    .as_str()
                    .filter(|text| !text.is_empty())
                    .map(str::to_owned)
            })
        })
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(count(pieces.len()), refs, "one reference per piece");
    assert_eq!(
        count(pieces.iter().collect::<BTreeSet<_>>().len()),
        texts,
        "one text per distinct piece"
    );

    let other = fixture()
        .into_iter()
        .map(|mut event| {
            event["event_id"] = json!(format!(
                "other-{}",
                event["event_id"].as_str().unwrap_or_default()
            ));
            event
        })
        .collect::<Vec<_>>();
    ingest(&mut database, OTHER_OWNER_ID, &other);
    assert_eq!(
        stored(&mut database, Table::Texts),
        texts.saturating_mul(2),
        "texts are not shared across owners"
    );
    // A duplicate event adds nothing.
    ingest(&mut database, OWNER_ID, &fixture());
    assert_eq!(stored(&mut database, Table::Refs), refs.saturating_mul(2));
}

#[test]
fn searches_are_scoped_to_the_owner() {
    let mut database = seeded();
    let found = search(&mut database, json!({"q": "nimbus"}));
    assert_eq!(
        found
            .items
            .iter()
            .map(|item| item.session_id.as_str())
            .collect::<Vec<_>>(),
        ["gamma", "alpha"],
        "the other owner's session is not found"
    );
    let other_session = session_pk(&mut database, OTHER_OWNER_ID, "other-session");
    let other = search_in_session(
        &mut database,
        OWNER_ID,
        other_session,
        &InSessionSearchQuery {
            q: "nimbus".to_owned(),
        },
        &StreamCache::default(),
    );
    assert!(
        matches!(other, Err(crate::error::AppError::NotFound(_))),
        "another owner's session is not found"
    );
    let unknown = search_in_session(
        &mut database,
        OWNER_ID,
        Uuid::from_u128(99),
        &InSessionSearchQuery {
            q: "nimbus".to_owned(),
        },
        &StreamCache::default(),
    );
    assert!(matches!(unknown, Err(crate::error::AppError::NotFound(_))));
    let empty = search_session(&mut database, "empty", "nimbus");
    assert!(empty.hits.is_empty() && empty.texts.is_empty() && !empty.truncated);
}

#[test]
fn hits_mark_new_and_inherited_pieces() {
    let mut database = seeded();
    let response = search_session(&mut database, "alpha", "NOTES.md");
    let events = stored_events(&mut database, OWNER_ID);
    // a2 replays [system, ask, grep, found]; only `found`, after the call, is new.
    let a2 = events
        .iter()
        .find(|event| event.event_id == "a2")
        .map(|event| event.pk)
        .expect("a2 is stored");
    let a2_hits = response
        .hits
        .iter()
        .filter(|hit| hit.event_pk == a2)
        .map(|hit| (hit.entry_index, hit.new))
        .collect::<Vec<_>>();
    assert_eq!(a2_hits, [(0, false), (1, false), (3, true)]);
    assert!(
        response.hits.iter().all(|hit| hit.segment.is_some()),
        "every alpha event is in an exchange"
    );

    let found = search(&mut database, json!({"q": "NOTES.md"}));
    let alpha = found
        .items
        .iter()
        .find(|item| item.session_id == "alpha")
        .expect("alpha matches");
    // a1 (all new), a2 (found is new), a3 (all inherited) hit; responses do not.
    assert_eq!((alpha.match_count, alpha.new_match_count), (3, 2));
    assert_eq!(
        alpha
            .matches
            .iter()
            .map(|matched| matched.new)
            .collect::<Vec<_>>(),
        [true, true, false]
    );
}

#[test]
fn a_tool_name_is_its_own_piece_and_orders_before_the_text() {
    let mut database = seeded();
    let response = search_session(&mut database, "beta", "read");
    let fields = response
        .hits
        .iter()
        .map(|hit| (hit.entry_index, hit.field.as_str()))
        .collect::<Vec<_>>();
    // b1r's call is entry 0; b2 replays it as entry 2.
    assert_eq!(fields, [(0, "name"), (2, "name")]);
    let across = search_session(&mut database, "alpha", "grep{");
    assert!(across.hits.is_empty(), "a name and a text are never joined");
}

#[test]
fn filters_narrow_the_matching_events() {
    let mut database = seeded();
    let sessions = |database: &mut SqliteConnection, query: Value| {
        search(database, query)
            .items
            .into_iter()
            .map(|item| (item.session_id, item.match_count))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        sessions(&mut database, json!({"q": "搜索"})),
        [("gamma".to_owned(), 1), ("alpha".to_owned(), 3)]
    );
    assert_eq!(
        sessions(
            &mut database,
            json!({"q": "搜索", "event_type": "response"})
        ),
        [("gamma".to_owned(), 1)]
    );
    assert_eq!(
        sessions(
            &mut database,
            json!({"q": "搜索", "from": "2026-09-01T00:00:03Z", "to": "2026-09-01T00:00:09Z"})
        ),
        [("alpha".to_owned(), 2)]
    );
    assert_eq!(
        sessions(
            &mut database,
            json!({"q": "搜索", "agent_name": ["claude-cli"]})
        ),
        []
    );
    assert_eq!(
        sessions(&mut database, json!({"q": "搜索", "agent_name": ["claude-cli", "codex_exec"], "llm_provider": ["openai"], "llm_model": ["gpt-5"]})).len(),
        2
    );
    assert_eq!(
        sessions(&mut database, json!({"q": "搜索", "llm_model": ["absent"]})),
        []
    );
}

#[test]
fn a_page_past_the_end_still_counts_the_sessions() {
    let mut database = seeded();
    let past_end = search(
        &mut database,
        json!({"q": "a", "page": 9_u32, "page_size": 1_u32}),
    );
    assert_eq!(past_end.total_sessions, 3);
    assert!(past_end.items.is_empty() && !past_end.has_more);
}

#[test]
fn a_nul_character_does_not_hide_later_trigrams() {
    let mut database = seeded();
    // b1 holds "a\0b\0c abc": the trigram index must still find "c abc".
    let hits = search_session(&mut database, "beta", "c abc").hits;
    assert_eq!(hits.len(), 2, "b1 and its replay in b2");
    let nul = search_session(&mut database, "beta", "a\0b\0c");
    assert_eq!(nul.hits.len(), 2, "a query with NUL matches literally");
}

#[test]
fn fragments_highlight_every_occurrence_in_their_window() {
    let mut database = seeded();
    let found = search(&mut database, json!({"q": "nimbus.example.org"}));
    let alpha = found
        .items
        .iter()
        .find(|item| item.session_id == "alpha")
        .expect("alpha matches");
    let rendered = alpha.matches[1].fragments[0]
        .segments
        .iter()
        .map(|segment| {
            if segment.highlighted {
                format!("[{}]", segment.text)
            } else {
                segment.text.clone()
            }
        })
        .collect::<String>();
    assert_eq!(
        rendered,
        "NOTES.md: [nimbus.example.org] 😀 [NIMBUS.EXAMPLE.ORG]"
    );
}
