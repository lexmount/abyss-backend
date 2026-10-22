//! Streams of one session: the facts ingest reads from each event, and the
//! exchanges, bases, and segments a query derives from them.
//!
//! A stream is one line of the model's context. Every request has at most one
//! base, the exchange whose output its context continues. Ingest reads the
//! roles, call ids, and item ids of one event's conversation entries, plus
//! whether a user entry has text, and never looks at another event. A query
//! groups one session's events into exchanges and finds each base by key
//! lookups within the session, without comparing content.

mod base;
mod facts;
#[cfg(test)]
mod fixtures;
mod segmentation;

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

pub use self::{
    base::{BaseRule, NoBaseReason},
    facts::{Label, RequestFacts, ResponseFacts},
    segmentation::SegmentKind,
};
use self::{
    base::{Exchange, emitters, find_base, group_exchanges},
    segmentation::{Segment, Segmentation},
};

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

#[derive(Debug, Serialize)]
/// Segments of one session, without their exchanges or any content.
pub struct SessionStreamsResponse {
    /// Session primary key.
    pub session_pk: Uuid,
    /// Segments by their first exchange.
    pub segments: Vec<StreamSegment>,
}

#[derive(Clone, Debug, Serialize)]
/// A run of exchanges along one stream.
pub struct StreamSegment {
    /// Id of the segment's first exchange, which identifies the segment.
    pub head: String,
    /// Why the segment begins.
    pub kind: SegmentKind,
    /// Head of the segment holding the first exchange's base.
    pub parent: Option<String>,
    /// Id of the first exchange's base.
    pub forked_from: Option<String>,
    /// Earliest event time of the segment.
    pub start: DateTime<Utc>,
    /// Latest event time of the segment.
    pub end: DateTime<Utc>,
    /// Number of exchanges in the segment.
    pub exchange_count: usize,
    /// The new user words of the segment's first request.
    pub label: Option<EntryLabel>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// Where a request's new user words are, and how they begin: the last
/// `user` entry with text in its new part.
pub struct EntryLabel {
    /// The request event.
    pub event_pk: Uuid,
    /// Index of the entry among the event's entries.
    pub entry_index: i32,
    /// At most the first 50 UTF-16 code units of the entry's text.
    pub text: String,
    /// Length of the entry's whole text in UTF-16 code units.
    pub length: i32,
}

impl EntryLabel {
    /// A session title or segment label from the same stored request facts.
    pub fn from_columns(
        event_pk: Option<Uuid>,
        entry_index: Option<i32>,
        text: Option<String>,
        length: Option<i32>,
    ) -> Option<Self> {
        let label = Label::from_columns(entry_index, text, length)?;
        Some(Self {
            event_pk: event_pk?,
            entry_index: label.index,
            text: label.text,
            length: label.length,
        })
    }
}

/// Immutable structure of one session at one stored revision. Contains stream
/// facts and short labels, never event bodies. Share it through `Arc`.
pub struct SessionStructure {
    exchanges: Vec<Exchange>,
    bases: Vec<Result<(usize, BaseRule), NoBaseReason>>,
    segmentation: Segmentation,
    segments: Vec<StreamSegment>,
    summary: StreamSummary,
    exchange_by_id: HashMap<String, usize>,
    event_to_exchange: HashMap<Uuid, usize>,
}

impl SessionStructure {
    /// Derives the streams of one session from its events.
    ///
    /// `events` are the session's events in session order: observation time,
    /// request before response, then primary key.
    pub fn derive(events: Vec<StreamEvent>) -> Self {
        let mut exchanges = group_exchanges(events);
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
        let summary = StreamSummary {
            exchanges: exchanges.len(),
            turns: exchanges
                .iter()
                .filter(|exchange| exchange.starts_turn())
                .count(),
            segments: segmentation.segments.len(),
        };
        let mut exchange_by_id = HashMap::with_capacity(exchanges.len());
        for (index, exchange) in exchanges.iter().enumerate() {
            exchange_by_id.entry(exchange.id.clone()).or_insert(index);
        }
        let event_to_exchange = exchanges
            .iter_mut()
            .enumerate()
            .flat_map(|(index, exchange)| {
                std::mem::take(&mut exchange.event_pks)
                    .into_iter()
                    .map(move |pk| (pk, index))
            })
            .collect();
        let mut structure = Self {
            exchanges,
            bases,
            segmentation,
            segments: Vec::new(),
            summary,
            exchange_by_id,
            event_to_exchange,
        };
        structure.segments = structure
            .segmentation
            .segments
            .iter()
            .map(|segment| structure.segment(segment))
            .collect();
        structure
    }

    /// The cached counts; no exchange traversal is needed.
    pub const fn summary(&self) -> StreamSummary {
        self.summary
    }

    /// Copies only the segment metadata returned by the stream endpoint.
    pub fn session_streams(&self, session_pk: Uuid) -> SessionStreamsResponse {
        SessionStreamsResponse {
            session_pk,
            segments: self.segments.clone(),
        }
    }

    /// Exchanges from `from` to the end of the segment currently holding it.
    pub fn segment_run(&self, from: &str) -> Option<SegmentRun> {
        let from = *self.exchange_by_id.get(from)?;
        let segment = self.segmentation.segment_of[from];
        let members = &self.segmentation.segments[segment].exchanges;
        let at = members.iter().position(|&exchange| exchange == from)?;
        Some(SegmentRun {
            segment: self.segment_head(segment).to_owned(),
            exchanges: members[at..]
                .iter()
                .map(|&exchange| self.exchange(exchange))
                .collect(),
        })
    }

    /// Current segment and exchange of a stable event anchor.
    pub fn event_position(&self, event_pk: Uuid) -> Option<EventLocation> {
        let exchange = *self.event_to_exchange.get(&event_pk)?;
        let segment = self.segmentation.segment_of[exchange];
        Some(EventLocation {
            segment: self.segment_head(segment).to_owned(),
            segment_index: segment.saturating_add(1),
            segment_kind: self.segments[segment].kind,
            exchange: self.exchanges[exchange].id.clone(),
        })
    }

    /// Location endpoint response, with the event and session identities.
    pub fn event_location(
        &self,
        session_pk: Uuid,
        event_pk: Uuid,
    ) -> Option<EventLocationResponse> {
        Some(EventLocationResponse {
            session_pk,
            event_pk,
            location: self.event_position(event_pk)?,
        })
    }

    /// Segment of one event, without building a map for unrelated events.
    #[cfg(feature = "sqlite-fts")]
    pub fn event_segment(&self, event_pk: Uuid) -> Option<&str> {
        let exchange = *self.event_to_exchange.get(&event_pk)?;
        Some(self.segment_head(self.segmentation.segment_of[exchange]))
    }

    /// Segment heads of the events that participate in the derived exchanges.
    #[cfg(test)]
    pub fn event_segments(&self) -> HashMap<Uuid, String> {
        self.event_to_exchange
            .iter()
            .map(|(&pk, &exchange)| {
                (
                    pk,
                    self.segment_head(self.segmentation.segment_of[exchange])
                        .to_owned(),
                )
            })
            .collect()
    }

    /// The label of an exchange's request.
    fn label(&self, exchange: usize) -> Option<EntryLabel> {
        let request = self.exchanges.get(exchange)?.request.as_ref()?;
        let label = request.facts.label.as_ref()?;
        Some(EntryLabel {
            event_pk: request.pk,
            entry_index: label.index,
            text: label.text.clone(),
            length: label.length,
        })
    }

    fn base(&self, exchange: usize) -> Option<usize> {
        self.bases[exchange].ok().map(|(base, _)| base)
    }

    fn segment(&self, segment: &Segment) -> StreamSegment {
        let id = |exchange: usize| self.exchanges[exchange].id.clone();
        let first = segment.exchanges.first().copied().unwrap_or_default();
        let members = segment
            .exchanges
            .iter()
            .map(|&exchange| &self.exchanges[exchange]);
        StreamSegment {
            head: id(first),
            kind: segment.kind,
            parent: segment
                .parent
                .and_then(|parent| self.segmentation.segments[parent].exchanges.first())
                .map(|&head| id(head)),
            forked_from: segment.parent.and_then(|_| self.base(first)).map(id),
            start: members
                .clone()
                .map(|exchange| exchange.at)
                .min()
                .unwrap_or_default(),
            end: members
                .map(|exchange| exchange.end)
                .max()
                .unwrap_or_default(),
            exchange_count: segment.exchanges.len(),
            label: self.label(first),
        }
    }
}

/// Derives the segments of one session from its events.
///
/// `events` are the session's events in session order: observation time,
/// request before response, then primary key.
#[cfg(test)]
pub fn session_streams(session_pk: Uuid, events: Vec<StreamEvent>) -> SessionStreamsResponse {
    SessionStructure::derive(events).session_streams(session_pk)
}

/// How many exchanges, turns, and segments one session's events form.
#[derive(Clone, Copy, Debug)]
pub struct StreamSummary {
    pub exchanges: usize,
    /// Requests that open a turn.
    pub turns: usize,
    pub segments: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
/// The current position of an event in its session's stream view.
pub struct EventLocation {
    /// The current segment head; it can change when a later fork is observed.
    pub segment: String,
    /// One-based position in the session's `streams.segments` array.
    pub segment_index: usize,
    /// Why this segment begins.
    pub segment_kind: SegmentKind,
    /// The exchange containing the event.
    pub exchange: String,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
/// The current location of an event in its owner's session structure.
pub struct EventLocationResponse {
    pub session_pk: Uuid,
    pub event_pk: Uuid,
    #[serde(flatten)]
    pub location: EventLocation,
}

#[derive(Debug, Serialize)]
/// One exchange of a segment, as the segment layer gives it.
pub struct SegmentExchange {
    /// Shared `response_id`, or the `event_id` of an event without one.
    pub id: String,
    /// Request event primary key.
    pub request_pk: Option<Uuid>,
    /// Response event primary key.
    pub response_pk: Option<Uuid>,
    /// Exchange the request's context continues.
    pub base: Option<String>,
    /// Head of the segment holding the base.
    pub base_segment: Option<String>,
    /// Rule that found the base.
    pub rule: Option<BaseRule>,
    /// Why there is no base.
    pub no_base_reason: Option<NoBaseReason>,
    /// Index of the request's entries where its new part begins.
    pub new_from: Option<i32>,
    /// Whether the request opens a turn.
    pub starts_turn: bool,
}

/// Exchanges of one segment from a given exchange on.
pub struct SegmentRun {
    /// Head of the segment that now holds the asked exchange.
    pub segment: String,
    /// The exchanges in order.
    pub exchanges: Vec<SegmentExchange>,
}

/// The exchanges of the segment holding exchange `from`, from it to the
/// segment's end. `None` when the session has no such exchange.
#[cfg(test)]
pub fn segment_run(events: Vec<StreamEvent>, from: &str) -> Option<SegmentRun> {
    SessionStructure::derive(events).segment_run(from)
}

/// The head of the segment holding each event's exchange, by event primary
/// key, including additional events of an exchange's already populated side.
#[cfg(test)]
pub fn event_segments(events: Vec<StreamEvent>) -> HashMap<Uuid, String> {
    SessionStructure::derive(events).event_segments()
}

impl SessionStructure {
    /// Id of a segment's first exchange.
    fn segment_head(&self, segment: usize) -> &str {
        let head = self.segmentation.segments[segment]
            .exchanges
            .first()
            .copied()
            .unwrap_or_default();
        &self.exchanges[head].id
    }

    fn exchange(&self, index: usize) -> SegmentExchange {
        let exchange = &self.exchanges[index];
        let base = self.base(index);
        let request = exchange.request.as_ref();
        SegmentExchange {
            id: exchange.id.clone(),
            request_pk: request.map(|request| request.pk),
            response_pk: exchange.response.as_ref().map(|response| response.pk),
            base: base.map(|base| self.exchanges[base].id.clone()),
            base_segment: base.map(|base| {
                self.segment_head(self.segmentation.segment_of[base])
                    .to_owned()
            }),
            rule: self.bases[index].ok().map(|(_, rule)| rule),
            no_base_reason: self.bases[index].err(),
            new_from: request.map(|request| request.facts.new_from),
            starts_turn: exchange.starts_turn(),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;
    use serde_json::json;
    use uuid::Uuid;

    use super::{BaseRule, SegmentKind, event_segments, segment_run, session_streams};
    use crate::usage::streams::fixtures::{Session, assistant, call, item, result, user};

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

        let streams = session_streams(Uuid::nil(), session.events);
        let segments = streams
            .segments
            .iter()
            .map(|segment| {
                (
                    segment.head.as_str(),
                    segment.kind,
                    segment.parent.as_deref(),
                    segment.forked_from.as_deref(),
                    segment.exchange_count,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            segments,
            [
                ("e1", SegmentKind::Root, None, None, 2),
                ("e3", SegmentKind::Turn, Some("e1"), Some("e2"), 1),
                ("e4", SegmentKind::Branch, Some("e3"), Some("e3"), 1),
                ("e5", SegmentKind::Branch, Some("e3"), Some("e3"), 1),
                ("e6", SegmentKind::Root, None, None, 1),
            ]
        );
        // A segment spans from its first request to its last response.
        let first = &streams.segments[0];
        assert_eq!(
            first.end.signed_duration_since(first.start),
            TimeDelta::milliseconds(1_500)
        );
    }
    #[test]
    fn a_segment_run_goes_from_its_exchange_to_the_segment_end() {
        let mut session = Session::new();
        session.exchange("e1", 1, &json!([user("go")]), &json!([call("a")]));
        session.exchange(
            "e2",
            2,
            &json!([user("go"), call("a"), result("a")]),
            &json!([call("b")]),
        );
        session.exchange(
            "e3",
            3,
            &json!([user("go"), call("a"), result("a"), call("b"), result("b")]),
            &json!([assistant("done")]),
        );
        let run = |from: &str| {
            segment_run(session.events.clone(), from).map(|run| {
                let ids = run
                    .exchanges
                    .iter()
                    .map(|exchange| exchange.id.as_str())
                    .collect::<Vec<_>>()
                    .join(",");
                (run.segment, ids)
            })
        };
        assert_eq!(run("e1"), Some(("e1".to_owned(), "e1,e2,e3".to_owned())));
        assert_eq!(
            run("e2"),
            Some(("e1".to_owned(), "e2,e3".to_owned())),
            "an exchange inside a segment runs from itself and names the head"
        );
        assert_eq!(run("zz"), None);

        let segment = segment_run(session.events.clone(), "e1").expect("a run");
        let e2 = &segment.exchanges[1];
        assert_eq!(
            (
                e2.base.as_deref(),
                e2.base_segment.as_deref(),
                e2.rule,
                e2.new_from
            ),
            (
                Some("e1"),
                Some("e1"),
                Some(BaseRule::LastOutput),
                Some(2_i32)
            )
        );
        assert!(e2.request_pk.is_some() && e2.response_pk.is_some());
        assert!(segment.exchanges[0].starts_turn && !e2.starts_turn);
    }

    #[test]
    fn every_event_of_an_exchange_maps_to_its_segment_head() {
        let mut session = Session::new();
        session.exchange("e1", 1, &json!([user("go")]), &json!([call("a")]));
        session.exchange(
            "e2",
            2,
            &json!([user("go"), call("a"), result("a")]),
            &json!([assistant("done")]),
        );
        session.exchange(
            "e3",
            3,
            &json!([user("unrelated")]),
            &json!([assistant("ok")]),
        );
        // A second request maps to e3 even though its content is not selected.
        session.ask("e3", 4, None, &json!([user("again")]));
        let segments = event_segments(session.events.clone());
        let heads = session
            .events
            .iter()
            .map(|event| segments.get(&event.pk).map(String::as_str))
            .collect::<Vec<_>>();
        assert_eq!(
            heads,
            [
                Some("e1"),
                Some("e1"),
                Some("e1"),
                Some("e1"),
                Some("e3"),
                Some("e3"),
                Some("e3")
            ]
        );
    }
}
