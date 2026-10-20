//! Streams of one session: the facts ingest reads from each event, and the
//! exchanges, bases, and segments a query derives from them.
//!
//! A stream is one line of the model's context. Every request has at most one
//! base, the exchange whose output its context continues. Ingest reads the
//! roles, call ids, and item ids of one event's conversation entries, plus
//! whether a user entry has text, and never looks at another event. A query
//! groups one session's events into exchanges and finds each base by key
//! lookups within the session, without comparing content.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use super::conversation::{Conversation, Entry};

/// Roles of the entries the model produced.
const MODEL_ROLES: [&str; 3] = ["assistant", "reasoning", "tool_call"];

/// Prefix that keeps item ids apart from call ids among entry identities.
const ITEM_PREFIX: &str = "item:";

impl Entry {
    fn is_model_side(&self) -> bool {
        MODEL_ROLES.contains(&self.role.as_str())
    }

    /// The item id of a model-side entry, with [`ITEM_PREFIX`].
    fn item(&self) -> Option<String> {
        self.item_id
            .as_deref()
            .filter(|_| self.is_model_side())
            .map(|item_id| format!("{ITEM_PREFIX}{item_id}"))
    }

    /// The provider-made ids of the entry: its item, then the call id of a
    /// `tool_call` entry.
    fn identities(&self) -> impl Iterator<Item = String> {
        let call = self.call_id.clone().filter(|_| self.role == "tool_call");
        self.item().into_iter().chain(call)
    }

    /// The entry's key in a skeleton: its call id, else its item.
    fn key(&self) -> Option<String> {
        self.call_id.clone().or_else(|| self.item())
    }
}

/// The entries of the event's conversation; none when it is not completely
/// decoded.
fn conversation_entries(metadata: &Value) -> Vec<Entry> {
    Conversation::read(metadata)
        .map(|conversation| conversation.entries)
        .unwrap_or_default()
}

/// One skeleton entry: an entry's role and key, its call id, else its
/// `item:`-prefixed item id.
pub type SkeletonEntry = (String, Option<String>);

/// The `(role, key)` of each entry in order.
fn skeleton(entries: &[Entry]) -> Vec<SkeletonEntry> {
    entries
        .iter()
        .map(|entry| (entry.role.clone(), entry.key()))
        .collect()
}

/// Call ids of the `tool_result` entries, in order.
fn result_ids(entries: &[Entry]) -> Vec<String> {
    entries
        .iter()
        .filter(|entry| entry.role == "tool_result")
        .filter_map(|entry| entry.call_id.clone())
        .collect()
}

/// Identities of the entries, in order.
fn identities(entries: &[Entry]) -> Vec<String> {
    entries.iter().flat_map(Entry::identities).collect()
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Facts of one request event, read from its conversation entries alone.
pub struct RequestFacts {
    /// Index after the last model-side entry, or 0 without one; the entries
    /// from here on are the request's new part.
    pub new_from: i32,
    /// The last identity in the last run, the run reaching from the last
    /// model-side entry back through model-side and `tool_result` entries to
    /// any other role or the start: the call id of a `tool_call` entry, else
    /// the `item:`-prefixed item id of a model-side entry.
    pub last_identity: Option<String>,
    /// Roles of the model-side entries after the one holding
    /// `last_identity`, in order; none of them has an identity. Empty without
    /// `last_identity`.
    pub trailing: Vec<String>,
    /// Without model-side entries, the call ids of every `tool_result`
    /// entry; otherwise empty.
    pub result_ids: Vec<String>,
    /// Whether the new part holds a `user` entry with text and no
    /// `tool_result` entry.
    pub starts_turn: bool,
}

impl RequestFacts {
    /// Reads the facts of a request event from its metadata.
    /// core logic of connect exchanges.
    pub fn read(metadata: &Value) -> Self {
        let entries = conversation_entries(metadata);
        let Some(last) = entries.iter().rposition(Entry::is_model_side) else {
            return Self {
                new_from: 0,
                last_identity: None,
                trailing: Vec::new(),
                result_ids: result_ids(&entries),
                starts_turn: starts_turn(&entries),
            };
        };
        let start = entries[..last]
            .iter()
            .rposition(|entry| !entry.is_model_side() && entry.role != "tool_result")
            .map_or(0, |other| other.saturating_add(1));
        let run = &entries[start..=last];
        let held = run
            .iter()
            .rposition(|entry| entry.identities().next().is_some());
        let last_identity = held.and_then(|held| run[held].identities().last());
        let trailing = held.map_or_else(Vec::new, |held| {
            run[held.saturating_add(1)..]
                .iter()
                .filter(|entry| entry.is_model_side())
                .map(|entry| entry.role.clone())
                .collect()
        });
        let new_from = last.saturating_add(1);
        Self {
            new_from: i32::try_from(new_from).unwrap_or(i32::MAX),
            last_identity,
            trailing,
            result_ids: Vec::new(),
            starts_turn: starts_turn(&entries[new_from..]),
        }
    }
}

/// Whether a new part opens a turn: it has user text and no tool result.
fn starts_turn(new_part: &[Entry]) -> bool {
    new_part
        .iter()
        .any(|entry| entry.role == "user" && !entry.text.is_empty())
        && !new_part.iter().any(|entry| entry.role == "tool_result")
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
/// Facts of one response event, read from its conversation entries alone.
pub struct ResponseFacts {
    /// Identities of every entry, in order: for each entry, the
    /// `item:`-prefixed item id of a model-side entry, then the call id of a
    /// `tool_call` entry.
    pub emitted_ids: Vec<String>,
    /// Skeleton of every entry.
    pub output_skeleton: Vec<SkeletonEntry>,
}

impl ResponseFacts {
    /// Reads the facts of a response event from its metadata.
    pub fn read(metadata: &Value) -> Self {
        let entries = conversation_entries(metadata);
        Self {
            emitted_ids: identities(&entries),
            output_skeleton: skeleton(&entries),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Stored stream facts of one event, by its side of the exchange.
pub enum EventFacts {
    /// The event is a request.
    Request(RequestFacts),
    /// The event is a response.
    Response(ResponseFacts),
}

#[derive(Clone, Debug)]
/// One session event with the stored columns streams are derived from.
pub struct StreamEvent {
    /// Event primary key.
    pub pk: Uuid,
    /// Collector-generated idempotency key.
    pub event_id: String,
    /// Collector observation time.
    pub observed_at: DateTime<Utc>,
    /// Shared by the request and response events of one exchange.
    pub response_id: Option<String>,
    /// Response the request names as the one it continues.
    pub previous_response_id: Option<String>,
    /// Facts read at ingest; an event stored before they were read has the
    /// default facts of its side.
    pub facts: EventFacts,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Rule that found a request's base.
pub enum BaseRule {
    /// The request names a response of the session in `previous_response_id`.
    Pointer,
    /// The exchange that emitted the request's last identity said every
    /// model-side entry that trails it.
    LastOutput,
    /// The request has no model output, and one exchange emitted every call
    /// its tool results answer.
    ResultsOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Why a request has no base.
pub enum NoBaseReason {
    /// The last model output has no identity, or model output its emitter
    /// did not say trails the last identity.
    TextOnly,
    /// The request has neither model output nor tool results.
    NoOutput,
    /// No other exchange of the session emitted the ids.
    UnknownIds,
    /// The ids come from more than one exchange.
    MultipleResponses,
    /// The exchange has no request event.
    MissingRequest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Why a segment begins.
pub enum SegmentKind {
    /// Its first exchange has no base.
    Root,
    /// Its first exchange continues a turn-opening request.
    Turn,
    /// Its first exchange continues a fork point; every branch is a new stream.
    Branch,
}

#[derive(Debug, Serialize)]
/// Exchanges and segments of one session, without any content.
pub struct SessionStreamsResponse {
    /// Session primary key.
    pub session_pk: Uuid,
    /// Exchanges by request time.
    pub exchanges: Vec<StreamExchange>,
    /// Segments by their first exchange.
    pub segments: Vec<StreamSegment>,
}

#[derive(Debug, Serialize)]
/// One request and its response.
pub struct StreamExchange {
    /// Shared `response_id`, or the `event_id` of an event without one.
    pub id: String,
    /// Request event primary key.
    pub request_pk: Option<Uuid>,
    /// Response event primary key.
    pub response_pk: Option<Uuid>,
    /// Exchange the request's context continues.
    pub base: Option<String>,
    /// Rule that found the base.
    pub rule: Option<BaseRule>,
    /// Why there is no base.
    pub no_base_reason: Option<NoBaseReason>,
    /// Index of the request's entries where its new part begins.
    pub new_from: Option<i32>,
    /// Index of the exchange's segment.
    pub segment: usize,
}

#[derive(Debug, Serialize)]
/// A run of exchanges along one stream.
pub struct StreamSegment {
    /// Why the segment begins.
    pub kind: SegmentKind,
    /// Segment of the first exchange's base.
    pub parent: Option<usize>,
    /// Exchange ids in order.
    pub exchanges: Vec<String>,
}

/// One side of an exchange as streams use it.
struct Side<F> {
    pk: Uuid,
    previous_response_id: Option<String>,
    facts: F,
}

struct Exchange {
    id: String,
    response_id: Option<String>,
    /// Request time, else response time.
    at: DateTime<Utc>,
    request: Option<Side<RequestFacts>>,
    response: Option<Side<ResponseFacts>>,
}

impl Exchange {
    /// Adds `event` unless the exchange already has an event of its side.
    fn add(&mut self, event: StreamEvent) {
        let (pk, previous_response_id) = (event.pk, event.previous_response_id);
        match event.facts {
            EventFacts::Request(facts) if self.request.is_none() => {
                self.at = event.observed_at;
                self.request = Some(Side {
                    pk,
                    previous_response_id,
                    facts,
                });
            }
            EventFacts::Response(facts) if self.response.is_none() => {
                self.response = Some(Side {
                    pk,
                    previous_response_id,
                    facts,
                });
            }
            EventFacts::Request(_) | EventFacts::Response(_) => {}
        }
    }

    fn starts_turn(&self) -> bool {
        self.request
            .as_ref()
            .is_some_and(|request| request.facts.starts_turn)
    }
}

/// Pairs events into exchanges by `response_id`, ordered by request time;
/// an event without a `response_id` is its own exchange.
fn group_exchanges(events: Vec<StreamEvent>) -> Vec<Exchange> {
    let mut exchanges: Vec<Exchange> = Vec::new();
    let mut by_response_id: HashMap<String, usize> = HashMap::new();
    for event in events {
        let known = event
            .response_id
            .as_ref()
            .and_then(|response_id| by_response_id.get(response_id).copied());
        let index = known.unwrap_or_else(|| {
            let id = event
                .response_id
                .clone()
                .unwrap_or_else(|| event.event_id.clone());
            if let Some(response_id) = &event.response_id {
                by_response_id.insert(response_id.clone(), exchanges.len());
            }
            exchanges.push(Exchange {
                id,
                response_id: event.response_id.clone(),
                at: event.observed_at,
                request: None,
                response: None,
            });
            exchanges.len().saturating_sub(1)
        });
        exchanges[index].add(event);
    }
    exchanges.sort_by_key(|exchange| exchange.at);
    exchanges
}

/// Exchanges whose response emitted each identity, in exchange order.
fn emitters(exchanges: &[Exchange]) -> HashMap<&str, Vec<usize>> {
    let mut by_id: HashMap<&str, Vec<usize>> = HashMap::new();
    for (index, exchange) in exchanges.iter().enumerate() {
        let Some(response) = &exchange.response else {
            continue;
        };
        for id in &response.facts.emitted_ids {
            let list = by_id.entry(id.as_str()).or_default();
            if list.last() != Some(&index) {
                list.push(index);
            }
        }
    }
    by_id
}

/// The one exchange other than `own` that emitted any of `ids`.
fn only_emitter<'a>(
    ids: impl IntoIterator<Item = &'a str>,
    own: usize,
    by_id: &HashMap<&str, Vec<usize>>,
) -> Result<usize, NoBaseReason> {
    let mut found = None;
    for id in ids {
        for &emitter in by_id.get(id).into_iter().flatten() {
            if emitter == own || found == Some(emitter) {
                continue;
            }
            if found.is_some() {
                return Err(NoBaseReason::MultipleResponses);
            }
            found = Some(emitter);
        }
    }
    found.ok_or(NoBaseReason::UnknownIds)
}

/// Whether `output` said every trailing role after its entry keyed by
/// `last`, in order: a client may drop entries of an output, but entries it
/// did not say come from a later reply.
fn said_after(output: &[SkeletonEntry], last: &str, trailing: &[String]) -> bool {
    let is_model_side = |role: &str| MODEL_ROLES.contains(&role);
    let Some(held) = output
        .iter()
        .rposition(|(role, key)| is_model_side(role) && key.as_deref() == Some(last))
    else {
        return false;
    };
    let mut said = output[held.saturating_add(1)..]
        .iter()
        .map(|(role, _)| role)
        .filter(|role| is_model_side(role));
    trailing
        .iter()
        .all(|role| said.any(|spoken| spoken == role))
}

/// The base of exchange `index` and the rule that found it: the response
/// its pointer names; else the one other exchange that emitted its last
/// identity, when that exchange said everything trailing it; else, without
/// model output, the one exchange that emitted the calls its tool results
/// answer.
fn find_base(
    index: usize,
    exchanges: &[Exchange],
    by_response_id: &HashMap<&str, usize>,
    by_id: &HashMap<&str, Vec<usize>>,
) -> Result<(usize, BaseRule), NoBaseReason> {
    let request = exchanges[index]
        .request
        .as_ref()
        .ok_or(NoBaseReason::MissingRequest)?;
    if let Some(&base) = request
        .previous_response_id
        .as_deref()
        .and_then(|response_id| by_response_id.get(response_id))
    {
        return Ok((base, BaseRule::Pointer));
    }
    let facts = &request.facts;
    if let Some(last) = facts.last_identity.as_deref() {
        let emitter = only_emitter([last], index, by_id)?;
        let output = exchanges[emitter]
            .response
            .as_ref()
            .map(|response| response.facts.output_skeleton.as_slice())
            .unwrap_or_default();
        return if said_after(output, last, &facts.trailing) {
            Ok((emitter, BaseRule::LastOutput))
        } else {
            Err(NoBaseReason::TextOnly)
        };
    }
    // The last run has model output without any identity.
    if facts.new_from > 0_i32 {
        return Err(NoBaseReason::TextOnly);
    }
    if facts.result_ids.is_empty() {
        return Err(NoBaseReason::NoOutput);
    }
    only_emitter(facts.result_ids.iter().map(String::as_str), index, by_id)
        .map(|base| (base, BaseRule::ResultsOnly))
}

/// Derives the exchanges and segments of one session from its events.
///
/// `events` are the session's events in session order: observation time,
/// request before response, then primary key.
pub fn session_streams(session_pk: Uuid, events: Vec<StreamEvent>) -> SessionStreamsResponse {
    let exchanges = group_exchanges(events);
    let by_response_id = exchanges
        .iter()
        .enumerate()
        .filter_map(|(index, exchange)| Some((exchange.response_id.as_deref()?, index)))
        .collect::<HashMap<_, _>>();
    let by_id = emitters(&exchanges);
    let bases = (0..exchanges.len())
        .map(|index| find_base(index, &exchanges, &by_response_id, &by_id))
        .collect::<Vec<_>>();
    let segmentation = Segmentation::cut(&exchanges, &bases);
    let segments = segmentation
        .segments
        .into_iter()
        .map(|segment| StreamSegment {
            kind: segment.kind,
            parent: segment.parent,
            exchanges: segment
                .exchanges
                .iter()
                .map(|&exchange| exchanges[exchange].id.clone())
                .collect(),
        })
        .collect();
    let exchanges = exchanges
        .iter()
        .zip(&bases)
        .zip(&segmentation.segment_of)
        .map(|((exchange, base), &segment)| StreamExchange {
            id: exchange.id.clone(),
            request_pk: exchange.request.as_ref().map(|request| request.pk),
            response_pk: exchange.response.as_ref().map(|response| response.pk),
            base: base.ok().map(|(base, _)| exchanges[base].id.clone()),
            rule: base.ok().map(|(_, rule)| rule),
            no_base_reason: base.err(),
            new_from: exchange
                .request
                .as_ref()
                .map(|request| request.facts.new_from),
            segment,
        })
        .collect();
    SessionStreamsResponse {
        session_pk,
        exchanges,
        segments,
    }
}

struct Segment {
    kind: SegmentKind,
    parent: Option<usize>,
    exchanges: Vec<usize>,
}

struct Segmentation {
    segments: Vec<Segment>,
    /// Segment of each exchange.
    segment_of: Vec<usize>,
}

impl Segmentation {
    /// Walks the exchanges by request time. A segment begins at an exchange
    /// without a base, at one whose base is a fork point, the base of more
    /// than one exchange, and at one whose request opens a turn; any other
    /// exchange joins its base's segment.
    fn cut(exchanges: &[Exchange], bases: &[Result<(usize, BaseRule), NoBaseReason>]) -> Self {
        let mut children = vec![0_usize; exchanges.len()];
        for &(base, _) in bases.iter().flatten() {
            children[base] = children[base].saturating_add(1);
        }
        let mut segments: Vec<Segment> = Vec::new();
        let mut placed: Vec<Option<usize>> = vec![None; exchanges.len()];
        for (index, exchange) in exchanges.iter().enumerate() {
            let base = bases[index].ok().map(|(base, _)| base);
            let base_segment = base.and_then(|base| placed[base]).zip(base);
            let segment = match base_segment {
                Some((segment, base)) if children[base] <= 1 && !exchange.starts_turn() => segment,
                Some((segment, base)) => {
                    let kind = if children[base] > 1 {
                        SegmentKind::Branch
                    } else {
                        SegmentKind::Turn
                    };
                    Self::open(&mut segments, kind, Some(segment))
                }
                None => Self::open(&mut segments, SegmentKind::Root, None),
            };
            segments[segment].exchanges.push(index);
            placed[index] = Some(segment);
        }
        Self {
            segments,
            segment_of: placed.into_iter().map(Option::unwrap_or_default).collect(),
        }
    }

    fn open(segments: &mut Vec<Segment>, kind: SegmentKind, parent: Option<usize>) -> usize {
        segments.push(Segment {
            kind,
            parent,
            exchanges: Vec::new(),
        });
        segments.len().saturating_sub(1)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeDelta, TimeZone as _, Utc};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{
        BaseRule, EventFacts, NoBaseReason, RequestFacts, ResponseFacts, SegmentKind,
        SessionStreamsResponse, StreamEvent, session_streams,
    };

    fn conversation(entries: &Value) -> Value {
        json!({"conversation": {"entries": entries, "completeness": "complete"}})
    }

    fn user(text: &str) -> Value {
        json!({"role": "user", "text": text})
    }

    fn system(text: &str) -> Value {
        json!({"role": "system", "text": text})
    }

    fn assistant(text: &str) -> Value {
        json!({"role": "assistant", "text": text})
    }

    fn reasoning() -> Value {
        json!({"role": "reasoning", "text": ""})
    }

    fn call(id: &str) -> Value {
        json!({"role": "tool_call", "text": "{}", "call_id": id, "name": "Bash"})
    }

    fn result(id: &str) -> Value {
        json!({"role": "tool_result", "text": "ok", "call_id": id})
    }

    /// `entry` as part of the model output item `id`.
    fn item(mut entry: Value, id: &str) -> Value {
        entry["item_id"] = json!(id);
        entry
    }

    fn skeleton(entries: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
        entries
            .iter()
            .map(|(role, id)| ((*role).to_owned(), id.map(str::to_owned)))
            .collect()
    }

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn request_facts_follow_the_last_run_and_the_new_part() {
        // A tool loop resent with its history: the last run walks back
        // through the client's tool result to the user entry.
        let facts = RequestFacts::read(&conversation(&json!([
            system("rules"),
            user("fix it"),
            reasoning(),
            call("a"),
            result("a"),
            reasoning(),
            call("b"),
            result("b")
        ])));
        assert_eq!(
            facts,
            RequestFacts {
                new_from: 7,
                last_identity: Some("b".to_owned()),
                trailing: Vec::new(),
                result_ids: Vec::new(),
                starts_turn: false,
            }
        );
        // New user text after the last answer opens a turn; the answer has no
        // identity, so it trails the last call.
        let facts = RequestFacts::read(&conversation(&json!([
            user("fix it"),
            call("a"),
            result("a"),
            assistant("fixed"),
            system("<reminder>"),
            user("next")
        ])));
        assert_eq!(facts.new_from, 4_i32);
        assert_eq!(facts.last_identity.as_deref(), Some("a"));
        assert_eq!(facts.trailing, ["assistant"]);
        assert!(facts.starts_turn);
        assert!(facts.result_ids.is_empty());
        // Without model output, every entry is new and tool results answer
        // calls that live elsewhere.
        assert_eq!(
            RequestFacts::read(&conversation(&json!([
                result("a"),
                result("b"),
                user("go")
            ]))),
            RequestFacts {
                new_from: 0,
                last_identity: None,
                trailing: Vec::new(),
                result_ids: ids(&["a", "b"]),
                starts_turn: false,
            }
        );
        // A run without identities has nothing to align on and nothing
        // trailing; the run stops at the user entry before it.
        let facts = RequestFacts::read(&conversation(&json!([
            call("a"),
            result("a"),
            user("thanks"),
            assistant("sure")
        ])));
        assert_eq!((facts.last_identity, facts.trailing), (None, Vec::new()));
        // Empty user text opens no turn; a request without a complete
        // conversation has no facts.
        assert!(!RequestFacts::read(&conversation(&json!([assistant("a"), user("")]))).starts_turn);
        assert_eq!(RequestFacts::read(&json!({})), RequestFacts::default());
        assert_eq!(
            RequestFacts::read(&json!({"conversation": {
                "entries": [assistant("a"), user("next")],
                "completeness": "partial"
            }})),
            RequestFacts::default()
        );
    }

    #[test]
    fn response_facts_list_emitted_calls_and_the_output_skeleton() {
        assert_eq!(
            ResponseFacts::read(&conversation(&json!([
                reasoning(),
                assistant("let me look"),
                call("a"),
                call("b"),
                {"role": "tool_result", "text": "found", "call_id": "s"}
            ]))),
            ResponseFacts {
                emitted_ids: ids(&["a", "b"]),
                output_skeleton: skeleton(&[
                    ("reasoning", None),
                    ("assistant", None),
                    ("tool_call", Some("a")),
                    ("tool_call", Some("b")),
                    ("tool_result", Some("s"))
                ]),
            }
        );
        assert_eq!(
            ResponseFacts::read(&json!({"conversation": {}})),
            ResponseFacts::default()
        );
    }

    #[test]
    fn identities_are_call_ids_and_prefixed_item_ids_of_model_output() {
        // OpenAI Responses: every output item carries its id, and a function
        // call its call id as well.
        let output = [
            item(reasoning(), "rs_1"),
            item(assistant("let me look"), "msg_1"),
            item(call("a"), "fc_1"),
        ];
        let facts = ResponseFacts::read(&conversation(&json!(output)));
        assert_eq!(
            facts.emitted_ids,
            ids(&["item:rs_1", "item:msg_1", "item:fc_1", "a"])
        );
        assert_eq!(
            facts.output_skeleton,
            skeleton(&[
                ("reasoning", Some("item:rs_1")),
                ("assistant", Some("item:msg_1")),
                ("tool_call", Some("a"))
            ]),
            "a skeleton keys entries by call id, else by item id"
        );
        // Entries of one item share its id; item ids of other roles are no
        // identities; a call's last identity is its call id.
        let mut request = vec![item(user("go"), "u_1")];
        request.extend(output.clone());
        request.extend([
            item(result("a"), "fco_1"),
            item(assistant("done,"), "msg_2"),
            item(assistant("really"), "msg_2"),
        ]);
        let facts = RequestFacts::read(&conversation(&json!(request)));
        assert_eq!(facts.last_identity.as_deref(), Some("item:msg_2"));
        assert!(facts.trailing.is_empty());
        let mut request = vec![user("go")];
        request.extend(output);
        request.extend([result("a"), reasoning(), assistant("done")]);
        let facts = RequestFacts::read(&conversation(&json!(request)));
        assert_eq!(facts.last_identity.as_deref(), Some("a"));
        assert_eq!(
            facts.trailing,
            ["reasoning", "assistant"],
            "trailing roles keep reasoning and leave out tool results"
        );
    }

    /// A session built from exchanges with conversation entries; exchange
    /// `name` shares `name` as its response id.
    struct Session {
        start: DateTime<Utc>,
        events: Vec<StreamEvent>,
    }

    impl Session {
        fn new() -> Self {
            Self {
                start: Utc
                    .with_ymd_and_hms(2026, 10, 20, 0, 0, 0)
                    .single()
                    .expect("valid time"),
                events: Vec::new(),
            }
        }

        fn add(&mut self, name: &str, millis: i64, previous: Option<&str>, facts: EventFacts) {
            let at = self
                .start
                .checked_add_signed(TimeDelta::milliseconds(millis))
                .expect("test times stay in range");
            self.events.push(StreamEvent {
                pk: Uuid::now_v7(),
                event_id: format!("{name}.{millis}"),
                observed_at: at,
                response_id: Some(name.to_owned()),
                previous_response_id: previous.map(str::to_owned),
                facts,
            });
        }

        /// Adds the request of exchange `name` at `second`.
        fn ask(&mut self, name: &str, second: i64, previous: Option<&str>, entries: &Value) {
            let facts = EventFacts::Request(RequestFacts::read(&conversation(entries)));
            self.add(name, second.saturating_mul(1_000), previous, facts);
        }

        /// Adds the response of exchange `name`, half a second after `second`.
        fn answer(&mut self, name: &str, second: i64, entries: &Value) {
            let facts = EventFacts::Response(ResponseFacts::read(&conversation(entries)));
            let millis = second.saturating_mul(1_000).saturating_add(500);
            self.add(name, millis, None, facts);
        }

        fn exchange(&mut self, name: &str, second: i64, request: &Value, response: &Value) {
            self.ask(name, second, None, request);
            self.answer(name, second, response);
        }

        fn streams(&self) -> SessionStreamsResponse {
            session_streams(Uuid::nil(), self.events.clone())
        }

        /// The base and rule of exchange `name`, or why it has none.
        fn base(&self, name: &str) -> Result<(String, BaseRule), NoBaseReason> {
            let streams = self.streams();
            let exchange = streams
                .exchanges
                .iter()
                .find(|exchange| exchange.id == name)
                .expect("the exchange exists");
            match (&exchange.base, exchange.rule, exchange.no_base_reason) {
                (Some(base), Some(rule), None) => Ok((base.clone(), rule)),
                (None, None, Some(reason)) => Err(reason),
                other => panic!("inconsistent base of {name}: {other:?}"),
            }
        }
    }

    fn linked(base: &str) -> (String, BaseRule) {
        (base.to_owned(), BaseRule::LastOutput)
    }

    #[test]
    fn a_request_continues_the_emitter_of_its_last_identity_unless_more_trails_it() {
        // The examples of STREAM-DESIGN "base 怎么判定": the outputs of the
        // earlier replies p1, p2, …, then what the next request carries back.
        let cases = [
            (
                "1: the call's result and new input do not count",
                vec![json!([assistant("look"), call("c1")])],
                json!([
                    user("go"),
                    assistant("look"),
                    call("c1"),
                    result("c1"),
                    user("more")
                ]),
                Ok(linked("p1")),
            ),
            (
                "2: rewritten text",
                vec![json!([assistant("check the log"), call("c1")])],
                json!([
                    user("go"),
                    assistant("see the log"),
                    call("c1"),
                    result("c1")
                ]),
                Ok(linked("p1")),
            ),
            (
                "3: a dropped entry",
                vec![json!([assistant("look"), call("c1")])],
                json!([user("go"), call("c1"), result("c1")]),
                Ok(linked("p1")),
            ),
            (
                "4: dropped thinking",
                vec![json!([reasoning(), call("c1")])],
                json!([user("go"), call("c1"), result("c1")]),
                Ok(linked("p1")),
            ),
            (
                "5: text p1 did not say",
                vec![json!([call("c1")])],
                json!([user("go"), call("c1"), result("c1"), assistant("fixed")]),
                Err(NoBaseReason::TextOnly),
            ),
            (
                "6: earlier calls of the run come from earlier replies",
                vec![json!([call("a")]), json!([call("b")])],
                json!([user("go"), call("a"), result("a"), call("b"), result("b")]),
                Ok(linked("p2")),
            ),
            (
                "7: a dropped server tool result",
                vec![json!([call("s1"), result("s1"), assistant("found")])],
                json!([user("go"), call("s1"), assistant("found")]),
                Ok(linked("p1")),
            ),
            (
                "8: text aligned on its item id",
                vec![json!([assistant("look"), item(assistant("so"), "msg_2")])],
                json!([user("go"), item(assistant("so"), "msg_2"), user("more")]),
                Ok(linked("p1")),
            ),
            (
                "9: dropped text after the call",
                vec![json!([call("c1"), assistant("done")])],
                json!([user("go"), call("c1"), result("c1")]),
                Ok(linked("p1")),
            ),
            (
                "thinking p1 did not say",
                vec![json!([call("c1"), assistant("done")])],
                json!([
                    user("go"),
                    call("c1"),
                    result("c1"),
                    reasoning(),
                    assistant("done")
                ]),
                Err(NoBaseReason::TextOnly),
            ),
            (
                "more text than p1 said",
                vec![json!([call("c1"), assistant("done")])],
                json!([
                    user("go"),
                    call("c1"),
                    assistant("done"),
                    assistant("again")
                ]),
                Err(NoBaseReason::TextOnly),
            ),
        ];
        for (case, outputs, request, expected) in cases {
            let mut session = Session::new();
            let mut second = 0_i64;
            for output in &outputs {
                second = second.saturating_add(1);
                session.exchange(&format!("p{second}"), second, &json!([user("go")]), output);
            }
            session.ask("next", second.saturating_add(1), None, &request);
            assert_eq!(session.base("next"), expected, "{case}");
        }
    }

    #[test]
    fn pointers_and_tool_results_find_a_base_and_requests_without_one_say_why() {
        let mut session = Session::new();
        session.exchange("p", 1, &json!([user("go")]), &json!([call("a"), call("b")]));
        session.exchange("twin", 2, &json!([user("go")]), &json!([call("d")]));
        session.exchange("other", 3, &json!([user("go")]), &json!([call("d")]));
        session.ask("pointer", 4, Some("p"), &json!([user("next")]));
        session.ask("results", 5, None, &json!([result("a"), result("b")]));
        session.ask(
            "plain_text",
            6,
            None,
            &json!([user("hi"), assistant("hey"), user("so")]),
        );
        session.ask("nothing", 7, None, &json!([user("hi")]));
        session.ask(
            "unknown",
            8,
            None,
            &json!([user("go"), call("zz"), result("zz")]),
        );
        session.ask("ambiguous", 9, None, &json!([result("d")]));
        session.answer("lonely", 10, &json!([assistant("hi")]));
        for (name, expected) in [
            ("pointer", Ok(("p".to_owned(), BaseRule::Pointer))),
            ("results", Ok(("p".to_owned(), BaseRule::ResultsOnly))),
            ("plain_text", Err(NoBaseReason::TextOnly)),
            ("nothing", Err(NoBaseReason::NoOutput)),
            ("unknown", Err(NoBaseReason::UnknownIds)),
            ("ambiguous", Err(NoBaseReason::MultipleResponses)),
            ("lonely", Err(NoBaseReason::MissingRequest)),
        ] {
            assert_eq!(session.base(name), expected, "{name}");
        }
    }

    #[test]
    fn segments_open_at_roots_turns_and_branches() {
        let mut session = Session::new();
        let asked = json!([user("fix it")]);
        let done = json!([user("fix it"), call("a"), result("a")]);
        let answered = json!([
            user("fix it"),
            call("a"),
            result("a"),
            item(assistant("done"), "msg_1"),
            user("next")
        ]);
        let mut branch = answered.as_array().cloned().expect("an entry list");
        branch.extend([call("b"), result("b")]);
        session.exchange("e1", 1, &asked, &json!([call("a")]));
        session.exchange("e2", 2, &done, &json!([item(assistant("done"), "msg_1")]));
        session.exchange("e3", 3, &answered, &json!([call("b")]));
        session.exchange("e4", 4, &json!(branch), &json!([assistant("one")]));
        session.exchange("e5", 5, &json!(branch), &json!([assistant("two")]));
        session.exchange(
            "e6",
            6,
            &json!([user("unrelated")]),
            &json!([assistant("ok")]),
        );

        let streams = session.streams();
        let segments = streams
            .segments
            .iter()
            .map(|segment| (segment.kind, segment.parent, segment.exchanges.join(",")))
            .collect::<Vec<_>>();
        assert_eq!(
            segments,
            [
                (SegmentKind::Root, None, "e1,e2".to_owned()),
                (SegmentKind::Turn, Some(0), "e3".to_owned()),
                (SegmentKind::Branch, Some(1), "e4".to_owned()),
                (SegmentKind::Branch, Some(1), "e5".to_owned()),
                (SegmentKind::Root, None, "e6".to_owned()),
            ]
        );
        let e3 = &streams.exchanges[2];
        assert_eq!((e3.segment, e3.new_from), (1, Some(4_i32)));
    }
}
