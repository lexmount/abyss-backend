//! Exchanges of one session and the base of each: the exchange whose output a
//! request's context continues, found by key lookups without comparing
//! content.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use super::{
    EventFacts, StreamEvent,
    facts::{MODEL_ROLES, RequestFacts, ResponseFacts, SkeletonEntry},
};

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

/// One side of an exchange as streams use it.
pub(super) struct Side<F> {
    pub(super) pk: Uuid,
    pub(super) previous_response_id: Option<String>,
    pub(super) facts: F,
}

/// `subagent_id`, `parent_subagent_id`, and `turn_id` of one event.
type DeclaredIds = (Option<String>, Option<String>, Option<String>);

pub(super) struct Exchange {
    pub(super) id: String,
    pub(super) response_id: Option<String>,
    /// Every event grouped into the exchange, including duplicate sides.
    /// Consumed when the session structure builds its event lookup.
    pub(super) event_pks: Vec<Uuid>,
    /// Request time, else response time.
    pub(super) at: DateTime<Utc>,
    /// Latest event time.
    pub(super) end: DateTime<Utc>,
    pub(super) request: Option<Side<RequestFacts>>,
    pub(super) response: Option<Side<ResponseFacts>>,
    /// Ids the client declared: the request's, else the response's.
    pub(super) subagent_id: Option<String>,
    pub(super) parent_subagent_id: Option<String>,
    pub(super) turn_id: Option<String>,
}

impl Exchange {
    /// Adds `event` unless the exchange already has an event of its side.
    fn add(&mut self, event: StreamEvent) {
        let (pk, previous_response_id) = (event.pk, event.previous_response_id);
        self.event_pks.push(pk);
        self.end = self.end.max(event.observed_at);
        let declared = (event.subagent_id, event.parent_subagent_id, event.turn_id);
        match event.facts {
            EventFacts::Request(facts) if self.request.is_none() => {
                self.at = event.observed_at;
                self.declare(declared);
                self.request = Some(Side {
                    pk,
                    previous_response_id,
                    facts,
                });
            }
            EventFacts::Response(facts) if self.response.is_none() => {
                if self.request.is_none() {
                    self.declare(declared);
                }
                self.response = Some(Side {
                    pk,
                    previous_response_id,
                    facts,
                });
            }
            EventFacts::Request(_) | EventFacts::Response(_) => {}
        }
    }

    fn declare(&mut self, (subagent, parent, turn): DeclaredIds) {
        self.subagent_id = subagent;
        self.parent_subagent_id = parent;
        self.turn_id = turn;
    }

    pub(super) fn starts_turn(&self) -> bool {
        self.request
            .as_ref()
            .is_some_and(|request| request.facts.starts_turn)
    }
}

/// Pairs events into exchanges by `response_id`, ordered by request time;
/// an event without a `response_id` is its own exchange.
pub(super) fn group_exchanges(events: Vec<StreamEvent>) -> Vec<Exchange> {
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
                event_pks: Vec::new(),
                at: event.observed_at,
                end: event.observed_at,
                request: None,
                response: None,
                subagent_id: None,
                parent_subagent_id: None,
                turn_id: None,
            });
            exchanges.len().saturating_sub(1)
        });
        exchanges[index].add(event);
    }
    exchanges.sort_by_key(|exchange| exchange.at);
    exchanges
}

/// Exchanges whose response emitted each identity, in exchange order.
pub(super) fn emitters(exchanges: &[Exchange]) -> HashMap<&str, Vec<usize>> {
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
pub(super) fn find_base(
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{BaseRule, NoBaseReason};
    use crate::usage::streams::fixtures::{
        Session, assistant, call, item, reasoning, result, user,
    };

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
}
