//! The turn view of one session: one node per turn, holding the groups of
//! the turn's exchanges, each with its main-line segment and side segments.

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use super::{EntryLabel, SessionStructure, segmentation::Group};

#[derive(Debug, Serialize)]
/// Turns of one session, without their exchanges or any content.
pub struct SessionTurnsResponse {
    /// Session primary key.
    pub session_pk: Uuid,
    /// Segments that belong to no turn first, then the turns by number.
    pub turns: Vec<TurnNode>,
}

#[derive(Debug, Serialize)]
/// One turn: the segments whose first exchange belongs to it.
pub struct TurnNode {
    /// One-based turn number; `None` for the segments of no turn.
    pub turn: Option<i64>,
    /// The declared value, or the id of the exchange that opened the turn.
    pub turn_id: Option<String>,
    /// Earliest event time of the turn's segments.
    pub start: DateTime<Utc>,
    /// Latest event time of the turn's segments.
    pub end: DateTime<Utc>,
    /// Number of exchanges in the turn's segments.
    pub exchange_count: usize,
    /// Label of the first labelled exchange on the main line of the first
    /// group: the main agent's when it takes part.
    pub label: Option<EntryLabel>,
    /// Whether the first executor's main line begins a new context: its first
    /// exchange has no base.
    pub new_context: bool,
    /// Executors of the turn as the tree of who started whom, depth first:
    /// the main line first, each executor followed by the sub-agents it
    /// started; siblings by their first segment's start. A sub-agent whose
    /// declared parent is not an executor of the turn sits at the top level.
    pub executors: Vec<TurnExecutor>,
}

#[derive(Debug, Serialize)]
/// The segments of one executor in one turn.
pub struct TurnExecutor {
    /// Sub-agent the segments declare; `None` for the main line.
    pub subagent_id: Option<String>,
    /// Sub-agent that started this one, as the first exchange of the
    /// executor's earliest segment declares it; `None` when the main agent
    /// started it, and for the main line.
    pub parent_subagent_id: Option<String>,
    /// Head of the group's main-line segment: every exchange of the group
    /// that is not a side exchange; empty when the group has none.
    pub main: Vec<String>,
    /// Heads of the group's side segments, one per side exchange, by start.
    pub side: Vec<String>,
    /// Number of exchanges in the executor's segments.
    pub exchange_count: usize,
    /// Earliest start of the executor's segments.
    pub start: DateTime<Utc>,
}

/// One executor's segments, as indices into the session's segments.
struct Executor {
    subagent_id: Option<String>,
    parent_subagent_id: Option<String>,
    main: Vec<usize>,
    side: Vec<usize>,
}

impl SessionStructure {
    /// The turn view: segments grouped by the turn of their first exchange,
    /// then by executor.
    pub fn session_turns(&self, session_pk: Uuid) -> SessionTurnsResponse {
        let mut by_turn: Vec<(Option<i64>, Vec<usize>)> = Vec::new();
        for (index, segment) in self.segments.iter().enumerate() {
            match by_turn.iter_mut().find(|(turn, _)| *turn == segment.turn) {
                Some((_, members)) => members.push(index),
                None => by_turn.push((segment.turn, vec![index])),
            }
        }
        // `None` sorts before every number.
        by_turn.sort_by_key(|(turn, _)| *turn);
        SessionTurnsResponse {
            session_pk,
            turns: by_turn
                .into_iter()
                .map(|(turn, members)| self.turn_node(turn, &members))
                .collect(),
        }
    }

    fn turn_node(&self, turn: Option<i64>, members: &[usize]) -> TurnNode {
        let segments = &self.segments;
        let executors = self.executors(members);
        let first = executors.first();
        let label = first.and_then(|executor| {
            executor.main.iter().find_map(|&segment| {
                self.segmentation.segments[segment]
                    .exchanges
                    .iter()
                    .find_map(|&exchange| self.label(exchange))
            })
        });
        let new_context = self.new_context(first.and_then(|executor| executor.main.first()));
        TurnNode {
            turn,
            turn_id: members
                .first()
                .and_then(|&segment| segments[segment].turn_id.clone()),
            start: members
                .iter()
                .map(|&segment| segments[segment].start)
                .min()
                .unwrap_or_default(),
            end: members
                .iter()
                .map(|&segment| segments[segment].end)
                .max()
                .unwrap_or_default(),
            exchange_count: self.exchange_count(members),
            label,
            new_context,
            executors: executors
                .into_iter()
                .map(|executor| {
                    let all = executor
                        .main
                        .iter()
                        .chain(&executor.side)
                        .copied()
                        .collect::<Vec<_>>();
                    TurnExecutor {
                        exchange_count: self.exchange_count(&all),
                        start: all
                            .iter()
                            .map(|&segment| segments[segment].start)
                            .min()
                            .unwrap_or_default(),
                        main: self.heads(&executor.main),
                        side: self.heads(&executor.side),
                        subagent_id: executor.subagent_id,
                        parent_subagent_id: executor.parent_subagent_id,
                    }
                })
                .collect(),
        }
    }

    /// The groups of one turn's segments (see `Segmentation::cut`), each
    /// holding its main-line segment and its side segments, in the
    /// depth-first order of who started whom.
    fn executors(&self, members: &[usize]) -> Vec<Executor> {
        let mut groups: Vec<(&Group, Vec<usize>)> = Vec::new();
        for &segment in members {
            let group = &self.segmentation.segments[segment].group;
            match groups.iter_mut().find(|(id, _)| *id == group) {
                Some((_, segments)) => segments.push(segment),
                None => groups.push((group, vec![segment])),
            }
        }
        for (_, segments) in &mut groups {
            segments.sort_by_key(|&segment| (self.segments[segment].start, segment));
        }
        groups.sort_by_key(|(group, segments)| {
            (
                matches!(group, Group::Declared(Some(_))),
                segments
                    .first()
                    .map(|&segment| self.segments[segment].start),
            )
        });
        let executors = groups
            .into_iter()
            .map(|(group, segments)| {
                let (side, main): (Vec<usize>, Vec<usize>) = segments
                    .iter()
                    .partition(|&&segment| self.segmentation.segments[segment].side);
                let subagent_id = match group {
                    Group::Declared(subagent) => subagent.clone(),
                    Group::Line(_) => None,
                };
                let parent_subagent_id = subagent_id.as_ref().and_then(|_| {
                    let first = *self.segmentation.segments[*segments.first()?]
                        .exchanges
                        .first()?;
                    self.exchanges[first].parent_subagent_id.clone()
                });
                Executor {
                    subagent_id,
                    parent_subagent_id,
                    main,
                    side,
                }
            })
            .collect();
        Self::started_first(executors)
    }

    /// Orders executors, already by start with the main line first, depth
    /// first by who started whom. An executor whose declared parent is not
    /// among them, or that a cycle of declarations reaches only through
    /// itself, sits at the top level in its place.
    fn started_first(executors: Vec<Executor>) -> Vec<Executor> {
        let parent_of = |index: usize| {
            let parent = executors[index].parent_subagent_id.as_deref()?;
            executors
                .iter()
                .position(|executor| executor.subagent_id.as_deref() == Some(parent))
                .filter(|&parent| parent != index)
        };
        let parents: Vec<Option<usize>> = (0..executors.len()).map(parent_of).collect();
        let mut order = Vec::with_capacity(executors.len());
        let mut placed = vec![false; executors.len()];
        // Top-level executors first; a cycle, never reached from the top,
        // is entered at its earliest member in the second pass.
        for top_only in [true, false] {
            for index in 0..executors.len() {
                if placed[index] || (top_only && parents[index].is_some()) {
                    continue;
                }
                let mut stack = vec![index];
                while let Some(next) = stack.pop() {
                    if placed[next] {
                        continue;
                    }
                    placed[next] = true;
                    order.push(next);
                    // Pushed in reverse so the earliest child comes out first.
                    stack.extend(
                        (0..executors.len())
                            .rev()
                            .filter(|&child| parents[child] == Some(next) && !placed[child]),
                    );
                }
            }
        }
        let mut slots: Vec<Option<Executor>> = executors.into_iter().map(Some).collect();
        order
            .into_iter()
            .filter_map(|index| slots[index].take())
            .collect()
    }

    /// Whether the first exchange of a turn's first main-line segment has no
    /// base.
    fn new_context(&self, first: Option<&usize>) -> bool {
        first
            .and_then(|&segment| self.segmentation.segments[segment].exchanges.first())
            .and_then(|&exchange| self.base(exchange))
            .is_none()
    }

    fn exchange_count(&self, segments: &[usize]) -> usize {
        segments
            .iter()
            .map(|&segment| self.segments[segment].exchange_count)
            .fold(0, usize::saturating_add)
    }

    fn heads(&self, segments: &[usize]) -> Vec<String> {
        segments
            .iter()
            .map(|&segment| self.segments[segment].head.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::SessionTurnsResponse;
    use crate::usage::streams::{
        SessionStructure,
        fixtures::{Session, assistant, call, result, user},
    };

    /// The entries of the main line up to and including the result of call
    /// `c{last}`.
    fn main_line(last: usize) -> Vec<Value> {
        let mut entries = vec![user("fix")];
        for index in 1..=last {
            let id = format!("c{index}");
            entries.extend([call(&id), result(&id)]);
        }
        entries
    }

    fn with(mut entries: Vec<Value>, more: Value) -> Value {
        entries.push(more);
        json!(entries)
    }

    /// `(turn, turn_id, new_context, [(subagent_id, main, side, exchange_count)])`.
    type Shape = (
        Option<i64>,
        Option<String>,
        bool,
        Vec<(Option<String>, Vec<String>, Vec<String>, usize)>,
    );

    fn shape_of(view: &SessionTurnsResponse) -> Vec<Shape> {
        view.turns
            .iter()
            .map(|turn| {
                (
                    turn.turn,
                    turn.turn_id.clone(),
                    turn.new_context,
                    turn.executors
                        .iter()
                        .map(|executor| {
                            (
                                executor.subagent_id.clone(),
                                executor.main.clone(),
                                executor.side.clone(),
                                executor.exchange_count,
                            )
                        })
                        .collect(),
                )
            })
            .collect()
    }

    #[test]
    fn turns_hold_executors_with_a_main_line_and_side_segments() {
        let mut session = Session::new();
        let p = |turn: &'static str| (None, None, Some(turn));
        // Before any declared turn.
        session.exchange("pre", 0, &json!([user("quota")]), &json!([assistant("ok")]));
        session.declared(
            "m1",
            1,
            &json!([user("fix")]),
            &json!([call("c1")]),
            p("p1"),
        );
        session.declared(
            "sub",
            2,
            &json!([user("explore")]),
            &json!([assistant("found")]),
            (Some("a1"), None, Some("p1")),
        );
        // A web search: a new root of the main line's executor.
        session.declared(
            "search",
            3,
            &json!([user("search the web")]),
            &json!([assistant("results")]),
            p("p1"),
        );
        session.declared("m2", 4, &json!(main_line(1)), &json!([call("c2")]), p("p1"));
        // A recap: a one-exchange fork of the main line.
        session.declared(
            "recap",
            5,
            &with(main_line(2), user("recap")),
            &json!([assistant("so far")]),
            p("p1"),
        );
        session.declared("m3", 6, &json!(main_line(2)), &json!([call("c3")]), p("p1"));
        session.declared("m4", 7, &json!(main_line(3)), &json!([call("c4")]), p("p1"));
        session.declared(
            "n1",
            8,
            &with(main_line(4), user("next")),
            &json!([assistant("done")]),
            p("p2"),
        );
        // The user rewinds to an earlier point of turn 1: still a continuation.
        // Its sibling m3 is continued in turn 1, but turn 3 has nothing else,
        // so r1 is turn 3's main line, not a side exchange.
        session.declared(
            "r1",
            9,
            &with(main_line(2), user("try again")),
            &json!([assistant("again")]),
            p("p3"),
        );
        let structure = SessionStructure::derive(session.events);
        let view = structure.session_turns(Uuid::nil());
        let strings = |heads: &[&str]| heads.iter().map(|head| (*head).to_owned()).collect();
        let shape = shape_of(&view);
        assert_eq!(
            shape,
            [
                (None, None, true, vec![(None, strings(&["pre"]), vec![], 1)]),
                (
                    Some(1),
                    Some("p1".to_owned()),
                    true,
                    vec![
                        // m1 to m4 form one main-line segment; the recap
                        // and the search are not continued.
                        (None, strings(&["m1"]), strings(&["search", "recap"]), 6),
                        (Some("a1".to_owned()), strings(&["sub"]), vec![], 1),
                    ]
                ),
                (
                    Some(2),
                    Some("p2".to_owned()),
                    false,
                    vec![(None, strings(&["n1"]), vec![], 1)]
                ),
                (
                    Some(3),
                    Some("p3".to_owned()),
                    false,
                    vec![(None, strings(&["r1"]), vec![], 1)]
                ),
            ]
        );
        let first = &view.turns[1];
        assert_eq!(
            first.label.as_ref().map(|label| label.text.as_str()),
            Some("fix")
        );
        assert_eq!(first.exchange_count, 7);
        let numbered = view.turns.iter().filter(|turn| turn.turn.is_some()).count();
        assert_eq!(numbered, structure.summary().turns);
    }

    #[test]
    fn executors_follow_who_started_whom() {
        let mut session = Session::new();
        let declare = |subagent: Option<&'static str>, parent: Option<&'static str>| {
            (subagent, parent, Some("p1"))
        };
        session.declared(
            "m1",
            1,
            &json!([user("fix")]),
            &json!([assistant("ok")]),
            declare(None, None),
        );
        session.declared(
            "a1",
            2,
            &json!([user("explore")]),
            &json!([assistant("a")]),
            declare(Some("a1"), None),
        );
        // Its declared parent takes no part in the turn: top level.
        session.declared(
            "c1",
            3,
            &json!([user("orphan")]),
            &json!([assistant("c")]),
            declare(Some("c1"), Some("gone")),
        );
        session.declared(
            "b1",
            4,
            &json!([user("search")]),
            &json!([assistant("b")]),
            declare(Some("b1"), None),
        );
        // Started by a1, after b1 began.
        session.declared(
            "a2",
            5,
            &json!([user("dig")]),
            &json!([assistant("d")]),
            declare(Some("a2"), Some("a1")),
        );
        session.declared(
            "a3",
            6,
            &json!([user("deeper")]),
            &json!([assistant("e")]),
            declare(Some("a3"), Some("a2")),
        );
        let view = SessionStructure::derive(session.events).session_turns(Uuid::nil());
        let executors = view.turns[0]
            .executors
            .iter()
            .map(|executor| {
                (
                    executor.subagent_id.as_deref(),
                    executor.parent_subagent_id.as_deref(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            executors,
            [
                (None, None),
                (Some("a1"), None),
                (Some("a2"), Some("a1")),
                (Some("a3"), Some("a2")),
                (Some("c1"), Some("gone")),
                (Some("b1"), None),
            ]
        );
    }
}
