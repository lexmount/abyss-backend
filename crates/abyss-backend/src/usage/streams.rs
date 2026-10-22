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

#[derive(Debug, Serialize)]
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

#[derive(Debug, Serialize)]
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

/// Exchanges of one session, the base of each, and the segments they form.
struct Streams {
    exchanges: Vec<Exchange>,
    bases: Vec<Result<(usize, BaseRule), NoBaseReason>>,
    segmentation: Segmentation,
}

impl Streams {
    /// Derives the streams of one session from its events.
    ///
    /// `events` are the session's events in session order: observation time,
    /// request before response, then primary key.
    fn derive(events: Vec<StreamEvent>) -> Self {
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
        Self {
            exchanges,
            bases,
            segmentation,
        }
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
pub fn session_streams(session_pk: Uuid, events: Vec<StreamEvent>) -> SessionStreamsResponse {
    let streams = Streams::derive(events);
    let segments = streams
        .segmentation
        .segments
        .iter()
        .map(|segment| streams.segment(segment))
        .collect();
    SessionStreamsResponse {
        session_pk,
        segments,
    }
}

/// How many exchanges, turns, and segments one session's events form, and
/// the new user words of its first request.
#[derive(Debug)]
pub struct StreamSummary {
    pub exchanges: usize,
    /// Requests that open a turn.
    pub turns: usize,
    pub segments: usize,
    pub title: Option<EntryLabel>,
}

/// Sums up the streams of one session's events.
pub fn stream_summary(events: Vec<StreamEvent>) -> StreamSummary {
    let streams = Streams::derive(events);
    StreamSummary {
        title: streams.label(0),
        exchanges: streams.exchanges.len(),
        turns: streams
            .exchanges
            .iter()
            .filter(|exchange| exchange.starts_turn())
            .count(),
        segments: streams.segmentation.segments.len(),
    }
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
pub fn segment_run(events: Vec<StreamEvent>, from: &str) -> Option<SegmentRun> {
    let streams = Streams::derive(events);
    let from = streams
        .exchanges
        .iter()
        .position(|exchange| exchange.id == from)?;
    let segment = streams.segmentation.segment_of[from];
    let members = &streams.segmentation.segments[segment].exchanges;
    let at = members.iter().position(|&exchange| exchange == from)?;
    Some(SegmentRun {
        segment: streams.segment_head(segment),
        exchanges: members[at..]
            .iter()
            .map(|&exchange| streams.exchange(exchange))
            .collect(),
    })
}

impl Streams {
    /// Id of a segment's first exchange.
    fn segment_head(&self, segment: usize) -> String {
        let head = self.segmentation.segments[segment]
            .exchanges
            .first()
            .copied()
            .unwrap_or_default();
        self.exchanges[head].id.clone()
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
            base_segment: base.map(|base| self.segment_head(self.segmentation.segment_of[base])),
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

    use super::{BaseRule, SegmentKind, segment_run, session_streams};
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
}
