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
    facts::{RequestFacts, ResponseFacts},
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

/// How many exchanges, turns, and segments one session's events form.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StreamCounts {
    pub exchanges: usize,
    /// Requests that open a turn.
    pub turns: usize,
    pub segments: usize,
}

/// Counts the exchanges, turns, and segments of one session's events.
pub fn stream_counts(events: Vec<StreamEvent>) -> StreamCounts {
    let streams = Streams::derive(events);
    StreamCounts {
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
pub struct SegmentPage {
    /// Head of the segment that now holds the asked exchange.
    pub segment: String,
    /// The exchanges in order.
    pub exchanges: Vec<SegmentExchange>,
    /// Exchange to continue after when the segment holds more.
    pub next: Option<String>,
}

/// The exchanges of the segment holding exchange `from`, from it on, or
/// after `after` when given, at most `limit`. `None` when the session has no
/// such exchange, or `after` is not in that segment from `from` on.
pub fn segment_page(
    events: Vec<StreamEvent>,
    from: &str,
    after: Option<&str>,
    limit: usize,
) -> Option<SegmentPage> {
    let streams = Streams::derive(events);
    let id = |exchange: usize| streams.exchanges[exchange].id.clone();
    let from = streams
        .exchanges
        .iter()
        .position(|exchange| exchange.id == from)?;
    let segment = &streams.segmentation.segments[streams.segmentation.segment_of[from]];
    let mut rest = &segment.exchanges[segment
        .exchanges
        .iter()
        .position(|&exchange| exchange == from)?..];
    if let Some(after) = after {
        let at = rest
            .iter()
            .position(|&exchange| streams.exchanges[exchange].id == after)?;
        rest = &rest[at.saturating_add(1)..];
    }
    let shown = &rest[..limit.min(rest.len())];
    Some(SegmentPage {
        segment: id(segment.exchanges.first().copied().unwrap_or_default()),
        exchanges: shown
            .iter()
            .map(|&exchange| streams.exchange(exchange))
            .collect(),
        next: (shown.len() < rest.len())
            .then(|| shown.last().copied().map(id))
            .flatten(),
    })
}

impl Streams {
    fn exchange(&self, index: usize) -> SegmentExchange {
        let exchange = &self.exchanges[index];
        let base = self.base(index);
        let segment_head = |segment: usize| {
            let head = self.segmentation.segments[segment]
                .exchanges
                .first()
                .copied();
            head.map(|head| self.exchanges[head].id.clone())
        };
        let request = exchange.request.as_ref();
        SegmentExchange {
            id: exchange.id.clone(),
            request_pk: request.map(|request| request.pk),
            response_pk: exchange.response.as_ref().map(|response| response.pk),
            base: base.map(|base| self.exchanges[base].id.clone()),
            base_segment: base.and_then(|base| segment_head(self.segmentation.segment_of[base])),
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

    use super::{BaseRule, SegmentKind, segment_page, session_streams};
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
    fn a_segment_page_runs_from_its_exchange_to_the_segment_end() {
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
        let page = |from: &str, after: Option<&str>, limit: usize| {
            segment_page(session.events.clone(), from, after, limit).map(|page| {
                let ids = page
                    .exchanges
                    .iter()
                    .map(|exchange| exchange.id.as_str())
                    .collect::<Vec<_>>()
                    .join(",");
                (page.segment, ids, page.next)
            })
        };
        let some = |segment: &str, ids: &str, next: Option<&str>| {
            Some((segment.to_owned(), ids.to_owned(), next.map(str::to_owned)))
        };
        assert_eq!(page("e1", None, 2), some("e1", "e1,e2", Some("e2")));
        assert_eq!(page("e1", Some("e2"), 2), some("e1", "e3", None));
        assert_eq!(
            page("e2", None, 50),
            some("e1", "e2,e3", None),
            "an exchange inside a segment pages from itself and names the head"
        );
        assert_eq!(page("zz", None, 50), None);
        assert_eq!(
            page("e2", Some("e1"), 50),
            None,
            "the cursor lies before the page"
        );

        let segment = segment_page(session.events.clone(), "e1", None, 50).expect("a page");
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
