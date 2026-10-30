//! Which hits are new, how matching sessions and their events are ordered,
//! and which events and texts a page of session search results shows.

use std::collections::{BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    search::{
        PieceField, SearchFragment, SessionSearchMatch, SessionSearchResponse, SessionSearchResult,
        ValidatedSearchQuery,
    },
    usage::UsageEventType,
};

/// Matching events a session result shows.
pub const MATCHES_PER_SESSION: usize = 3;
/// Fragments a matching event shows.
pub const FRAGMENTS_PER_MATCH: usize = 2;

/// Whether a hit is outside the event's inherited context: every hit of a
/// response, and the hits of a request from its `new_from` on, or from its
/// first entry without one.
#[must_use]
pub fn is_new(event_type: UsageEventType, new_from: Option<u32>, entry_index: u32) -> bool {
    event_type == UsageEventType::Response || entry_index >= new_from.unwrap_or_default()
}

/// One hit of a session search, with the event facts ranking reads.
#[derive(Clone, Debug)]
pub struct SearchHit {
    /// Session of the event.
    pub session_pk: Uuid,
    /// Agent-native identifier of that session.
    pub session_id: String,
    /// Event holding the piece.
    pub event_pk: Uuid,
    /// Observation time of the event.
    pub observed_at: DateTime<Utc>,
    /// Position of the piece's entry.
    pub entry_index: u32,
    /// Which value of the entry the piece is.
    pub field: PieceField,
    /// Text of the piece.
    pub text_id: i64,
    /// Whether the hit is new, by [`is_new`].
    pub new: bool,
}

/// One page of matching sessions, before the event store hydrates it.
#[derive(Debug, PartialEq, Eq)]
pub struct RankedPage {
    /// Sessions with at least one matching event.
    pub total_sessions: u64,
    /// The page's sessions, most recently matching first.
    pub sessions: Vec<RankedSession>,
}

/// One matching session of a page.
#[derive(Debug, PartialEq, Eq)]
pub struct RankedSession {
    /// Session primary key.
    pub session_pk: Uuid,
    /// Matching events.
    pub match_count: u64,
    /// Matching events with a new hit.
    pub new_match_count: u64,
    /// The matching events shown, at most [`MATCHES_PER_SESSION`].
    pub matches: Vec<RankedMatch>,
}

/// One matching event shown for a session.
#[derive(Debug, PartialEq, Eq)]
pub struct RankedMatch {
    /// The event.
    pub event_pk: Uuid,
    /// Observation time of the event.
    pub observed_at: DateTime<Utc>,
    /// Whether the event has a new hit.
    pub new: bool,
    /// Texts its fragments come from, at most [`FRAGMENTS_PER_MATCH`].
    pub fragment_texts: Vec<i64>,
}

/// Event columns a shown match reports.
pub struct MatchedEvent {
    /// Request or response side.
    pub event_type: String,
    /// LLM provider as reported.
    pub llm_provider: String,
    /// Model as reported.
    pub llm_model: String,
}

/// Session and device columns a session result reports.
pub struct SessionDetails {
    /// Agent-native session identifier.
    pub session_id: String,
    /// Agent name as reported by the collector.
    pub agent_name: String,
    /// Most recently observed Agent version.
    pub agent_version: Option<String>,
    /// Authoritative device host name.
    pub host_name: String,
    /// Authoritative device platform.
    pub platform: String,
    /// Earliest event observation in the session.
    pub started_at: DateTime<Utc>,
    /// Latest event observation in the session.
    pub ended_at: Option<DateTime<Utc>>,
}

/// The hits of one event.
struct EventHits {
    session_pk: Uuid,
    session_id: String,
    observed_at: DateTime<Utc>,
    hits: Vec<PieceHit>,
}

#[derive(Clone, Copy)]
struct PieceHit {
    new: bool,
    entry_index: u32,
    field: PieceField,
    text_id: i64,
}

impl EventHits {
    fn has_new(&self) -> bool {
        self.hits.iter().any(|hit| hit.new)
    }

    /// Distinct texts of the hits: new hits first, then those in the
    /// inherited context, each by entry and `name` before `text`.
    fn fragment_texts(&self) -> Vec<i64> {
        let mut hits = self.hits.clone();
        hits.sort_by_key(|hit| (!hit.new, hit.entry_index, hit.field));
        let mut texts = Vec::with_capacity(FRAGMENTS_PER_MATCH);
        for hit in hits {
            if texts.len() == FRAGMENTS_PER_MATCH {
                break;
            }
            if !texts.contains(&hit.text_id) {
                texts.push(hit.text_id);
            }
        }
        texts
    }
}

/// Groups hits into matching events and sessions and keeps one page.
///
/// Sessions order by their latest matching event, newest first, then by
/// session id, then by session key. A session shows its matching events with a new hit, earliest
/// first, then fills up with its earliest events whose hits are all in the
/// inherited context; events at the same time order by key.
#[must_use]
pub fn rank_sessions(hits: Vec<SearchHit>, offset: usize, page_size: usize) -> RankedPage {
    let mut events = HashMap::<Uuid, EventHits>::new();
    for hit in hits {
        let SearchHit {
            session_pk,
            session_id,
            event_pk,
            observed_at,
            entry_index,
            field,
            text_id,
            new,
        } = hit;
        events
            .entry(event_pk)
            .or_insert_with(|| EventHits {
                session_pk,
                session_id,
                observed_at,
                hits: Vec::new(),
            })
            .hits
            .push(PieceHit {
                new,
                entry_index,
                field,
                text_id,
            });
    }
    let mut sessions = HashMap::<Uuid, Vec<(Uuid, EventHits)>>::new();
    for (event_pk, event) in events {
        sessions
            .entry(event.session_pk)
            .or_default()
            .push((event_pk, event));
    }
    let mut sessions = sessions
        .into_iter()
        .map(|(session_pk, events)| {
            let latest = events.iter().map(|(_, event)| event.observed_at).max();
            let session_id = events
                .first()
                .map(|(_, event)| event.session_id.clone())
                .unwrap_or_default();
            ((latest, session_id, session_pk), events)
        })
        .collect::<Vec<_>>();
    sessions.sort_by(
        |((left_latest, left_id, left_pk), _), ((right_latest, right_id, right_pk), _)| {
            right_latest
                .cmp(left_latest)
                .then_with(|| left_id.cmp(right_id))
                .then_with(|| left_pk.cmp(right_pk))
        },
    );
    RankedPage {
        total_sessions: count(sessions.len()),
        sessions: sessions
            .into_iter()
            .skip(offset)
            .take(page_size)
            .map(|((_, _, session_pk), events)| rank_session(session_pk, events))
            .collect(),
    }
}

fn rank_session(session_pk: Uuid, mut events: Vec<(Uuid, EventHits)>) -> RankedSession {
    events.sort_by_key(|(event_pk, event)| (event.observed_at, *event_pk));
    let new_match_count = events.iter().filter(|(_, event)| event.has_new()).count();
    let matches = events
        .iter()
        .filter(|(_, event)| event.has_new())
        .chain(events.iter().filter(|(_, event)| !event.has_new()))
        .take(MATCHES_PER_SESSION)
        .map(|(event_pk, event)| RankedMatch {
            event_pk: *event_pk,
            observed_at: event.observed_at,
            new: event.has_new(),
            fragment_texts: event.fragment_texts(),
        })
        .collect();
    RankedSession {
        session_pk,
        match_count: count(events.len()),
        new_match_count: count(new_match_count),
        matches,
    }
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

impl RankedPage {
    /// Session keys of the page, in order.
    #[must_use]
    pub fn session_pks(&self) -> Vec<Uuid> {
        self.sessions
            .iter()
            .map(|session| session.session_pk)
            .collect()
    }

    /// Every shown event.
    #[must_use]
    pub fn event_pks(&self) -> Vec<Uuid> {
        self.matches().map(|matched| matched.event_pk).collect()
    }

    /// Every text a shown fragment comes from, once.
    #[must_use]
    pub fn fragment_texts(&self) -> Vec<i64> {
        self.matches()
            .flat_map(|matched| matched.fragment_texts.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn matches(&self) -> impl Iterator<Item = &RankedMatch> {
        self.sessions
            .iter()
            .flat_map(|session| session.matches.iter())
    }

    /// Hydrates the page with its sessions' details, its events' columns,
    /// and one fragment per text. A session or event the store no longer
    /// holds is left out. A session lists the providers and models of the
    /// matches it shows.
    #[must_use]
    pub fn into_response(
        self,
        query: ValidatedSearchQuery,
        mut details: HashMap<Uuid, SessionDetails>,
        events: &HashMap<Uuid, MatchedEvent>,
        fragments: &HashMap<i64, SearchFragment>,
    ) -> SessionSearchResponse {
        let items = self
            .sessions
            .into_iter()
            .filter_map(|ranked| {
                let details = details.remove(&ranked.session_pk)?;
                let matches = ranked
                    .matches
                    .into_iter()
                    .filter_map(|matched| {
                        let event = events.get(&matched.event_pk)?;
                        Some(SessionSearchMatch {
                            event_pk: matched.event_pk,
                            location: None,
                            event_type: event.event_type.clone(),
                            llm_provider: event.llm_provider.clone(),
                            llm_model: event.llm_model.clone(),
                            observed_at: matched.observed_at,
                            new: matched.new,
                            fragments: matched
                                .fragment_texts
                                .iter()
                                .filter_map(|text_id| fragments.get(text_id).cloned())
                                .collect(),
                        })
                    })
                    .collect::<Vec<_>>();
                Some(SessionSearchResult {
                    session_pk: ranked.session_pk,
                    title: None,
                    session_id: details.session_id,
                    agent_name: details.agent_name,
                    agent_version: details.agent_version,
                    host_name: details.host_name,
                    platform: details.platform,
                    started_at: details.started_at,
                    ended_at: details.ended_at,
                    providers: sorted_distinct(matches.iter().map(|m| &m.llm_provider)),
                    models: sorted_distinct(matches.iter().map(|m| &m.llm_model)),
                    match_count: ranked.match_count,
                    new_match_count: ranked.new_match_count,
                    matches,
                })
            })
            .collect();
        let consumed = u64::from(query.offset).saturating_add(u64::from(query.page_size));
        SessionSearchResponse {
            query: query.text,
            total_sessions: self.total_sessions,
            page: query.page,
            page_size: query.page_size,
            has_more: consumed < self.total_sessions,
            items,
        }
    }
}

fn sorted_distinct<'a>(values: impl Iterator<Item = &'a String>) -> Vec<String> {
    let mut values = values
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    values.sort();
    values
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone as _, Utc};
    use uuid::Uuid;

    use std::collections::HashMap;

    use super::{
        MatchedEvent, RankedMatch, RankedPage, SearchHit, SessionDetails, is_new, rank_sessions,
    };
    use crate::{
        search::{
            PieceField, SearchFragment, SearchFragmentSegment, SessionSearchQuery,
            ValidatedSearchQuery,
        },
        usage::UsageEventType,
    };

    fn at(second: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 18, 0, 0, second)
            .single()
            .expect("valid time")
    }

    /// A session key standing for a session name of at most 16 bytes.
    fn pk(session: &str) -> Uuid {
        Uuid::from_u128(
            session
                .bytes()
                .fold(0_u128, |key, byte| (key << 8_u32) | u128::from(byte)),
        )
    }

    fn names(page: &RankedPage, sessions: &[&str]) -> Vec<String> {
        page.session_pks()
            .iter()
            .map(|session_pk| {
                sessions
                    .iter()
                    .find(|session| pk(session) == *session_pk)
                    .map(|session| (*session).to_owned())
                    .unwrap_or_default()
            })
            .collect()
    }

    fn hit(
        session: &str,
        event: u128,
        second: u32,
        entry: u32,
        text_id: i64,
        new: bool,
    ) -> SearchHit {
        SearchHit {
            session_pk: pk(session),
            session_id: session.to_owned(),
            event_pk: Uuid::from_u128(event),
            observed_at: at(second),
            entry_index: entry,
            field: PieceField::Text,
            text_id,
            new,
        }
    }

    #[test]
    fn hits_are_new_in_responses_and_from_a_requests_new_from() {
        assert!(is_new(UsageEventType::Response, None, 0));
        assert!(
            is_new(UsageEventType::Response, Some(9), 0),
            "a response has no context"
        );
        assert!(!is_new(UsageEventType::Request, Some(4), 3));
        assert!(is_new(UsageEventType::Request, Some(4), 4));
        assert!(is_new(UsageEventType::Request, Some(0), 0));
        assert!(
            is_new(UsageEventType::Request, None, 0),
            "a request without new_from is all new"
        );
    }

    #[test]
    fn sessions_order_by_their_latest_matching_event_then_by_id() {
        let hits = vec![
            hit("old", 1, 1, 0, 1, true),
            hit("tie-b", 2, 5, 0, 1, true),
            hit("tie-a", 3, 5, 0, 1, false),
            hit("old", 4, 2, 0, 1, true),
            hit("new", 5, 9, 0, 1, false),
            hit("new", 6, 0, 0, 1, true),
        ];
        let all = ["old", "tie-a", "tie-b", "new"];
        let page = rank_sessions(hits.clone(), 0, 10);
        assert_eq!(page.total_sessions, 4);
        assert_eq!(names(&page, &all), ["new", "tie-a", "tie-b", "old"]);

        let second = rank_sessions(hits.clone(), 1, 2);
        assert_eq!(second.total_sessions, 4);
        assert_eq!(names(&second, &all), ["tie-a", "tie-b"]);
        assert_eq!(rank_sessions(hits, 4, 2).sessions, []);
    }

    #[test]
    fn sessions_sharing_an_id_stay_apart_and_order_by_key() {
        let mut later = hit("b", 1, 5, 0, 1, true);
        later.session_id = "same".to_owned();
        let mut earlier = hit("a", 2, 5, 0, 1, true);
        earlier.session_id = "same".to_owned();
        let page = rank_sessions(vec![later, earlier], 0, 10);
        assert_eq!(page.total_sessions, 2);
        assert_eq!(names(&page, &["a", "b"]), ["a", "b"]);
    }

    #[test]
    fn a_session_shows_new_matches_first_then_its_earliest_context_matches() {
        let hits = vec![
            hit("s", 1, 1, 0, 10, false),
            hit("s", 2, 2, 5, 11, true),
            hit("s", 2, 2, 0, 10, false),
            hit("s", 3, 3, 0, 10, false),
            hit("s", 4, 4, 1, 12, true),
            hit("s", 5, 4, 0, 10, false),
        ];
        let page = rank_sessions(hits, 0, 10);
        let session = &page.sessions[0];
        assert_eq!((session.match_count, session.new_match_count), (5, 2));
        let shown = session
            .matches
            .iter()
            .map(|matched| (matched.event_pk.as_u128(), matched.new))
            .collect::<Vec<_>>();
        assert_eq!(shown, [(2, true), (4, true), (1, false)]);

        let all_context = rank_sessions(
            vec![
                hit("c", 9, 3, 0, 1, false),
                hit("c", 8, 3, 0, 1, false),
                hit("c", 7, 1, 0, 1, false),
                hit("c", 6, 2, 0, 1, false),
            ],
            0,
            1,
        );
        let shown = all_context.sessions[0]
            .matches
            .iter()
            .map(|matched| matched.event_pk.as_u128())
            .collect::<Vec<_>>();
        assert_eq!(shown, [7, 6, 8], "equal times order by event key");
    }

    #[test]
    fn fragments_come_from_distinct_texts_new_hits_first() {
        let mut name = hit("s", 1, 1, 3, 30, true);
        name.field = PieceField::Name;
        let hits = vec![
            hit("s", 1, 1, 0, 10, false),
            hit("s", 1, 1, 3, 31, true),
            name,
            hit("s", 1, 1, 4, 30, true),
            hit("s", 2, 2, 0, 10, false),
            hit("s", 2, 2, 1, 10, false),
            hit("s", 2, 2, 2, 20, false),
        ];
        let page = rank_sessions(hits, 0, 10);
        let texts = page.sessions[0]
            .matches
            .iter()
            .map(|matched: &RankedMatch| matched.fragment_texts.clone())
            .collect::<Vec<_>>();
        assert_eq!(texts, [vec![30, 31], vec![10, 20]]);
        assert_eq!(page.fragment_texts(), [10, 20, 30, 31]);
        assert_eq!(page.event_pks(), [Uuid::from_u128(1), Uuid::from_u128(2)]);
    }

    fn details(session_id: &str) -> SessionDetails {
        SessionDetails {
            session_id: session_id.to_owned(),
            agent_name: "codex".to_owned(),
            agent_version: None,
            host_name: "alice-mbp".to_owned(),
            platform: "macos".to_owned(),
            started_at: at(0),
            ended_at: Some(at(9)),
        }
    }

    fn query(page: u32, page_size: u32) -> ValidatedSearchQuery {
        SessionSearchQuery {
            q: "needle".to_owned(),
            from: None,
            to: None,
            agent_name: Vec::new(),
            llm_provider: Vec::new(),
            llm_model: Vec::new(),
            event_type: None,
            page: Some(page),
            page_size: Some(page_size),
        }
        .validate()
        .expect("query is valid")
    }

    #[test]
    fn a_page_is_hydrated_from_the_store_and_drops_what_it_lacks() {
        let hits = vec![
            hit("kept", 1, 5, 0, 10, true),
            hit("kept", 2, 6, 0, 11, false),
            hit("gone", 3, 7, 0, 10, true),
            hit("later", 4, 1, 0, 10, true),
        ];
        let page = rank_sessions(hits, 0, 2);
        assert_eq!(names(&page, &["gone", "kept"]), ["gone", "kept"]);
        let fragment = |text: &str| SearchFragment {
            segments: vec![SearchFragmentSegment {
                text: text.to_owned(),
                highlighted: true,
            }],
        };
        let events = HashMap::from([(
            uuid::Uuid::from_u128(1),
            MatchedEvent {
                event_type: "request".to_owned(),
                llm_provider: "openai".to_owned(),
                llm_model: "gpt-5".to_owned(),
            },
        )]);
        let fragments = HashMap::from([(10, fragment("needle"))]);
        let response = page.into_response(
            query(1, 2),
            HashMap::from([(pk("kept"), details("kept"))]),
            &events,
            &fragments,
        );

        assert_eq!(response.total_sessions, 3);
        assert!(response.has_more, "a third session follows the page");
        assert_eq!(
            response.items.len(),
            1,
            "a session without details is left out"
        );
        let kept = &response.items[0];
        assert_eq!((kept.match_count, kept.new_match_count), (2, 1));
        assert_eq!(
            kept.matches
                .iter()
                .map(|matched| (
                    matched.event_pk.as_u128(),
                    matched.new,
                    matched.fragments.len()
                ))
                .collect::<Vec<_>>(),
            [(1, true, 1)],
            "an event the store lacks is left out"
        );
        assert_eq!(kept.matches[0].llm_model, "gpt-5");
        assert_eq!(
            (kept.session_id.as_str(), kept.models.as_slice()),
            ("kept", ["gpt-5".to_owned()].as_slice()),
            "a session lists the models of the matches it shows"
        );
        assert_eq!(kept.matches[0].fragments[0], fragment("needle"));
    }
}
