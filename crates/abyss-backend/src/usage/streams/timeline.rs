//! The turn view of one session: one node per turn, holding the segments of
//! each executor in the turn, split into a main line and side segments.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use super::{EntryLabel, SessionStructure};

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
    /// Label of the earliest labelled segment on the main line of the first
    /// executor: the main line (no sub-agent) when it takes part.
    pub label: Option<EntryLabel>,
    /// Whether the first executor's main line begins a new context: its first
    /// exchange has no base.
    pub new_context: bool,
    /// Executors of the turn: the main line first, then sub-agents by their
    /// first segment's start.
    pub executors: Vec<TurnExecutor>,
}

#[derive(Debug, Serialize)]
/// The segments of one executor in one turn.
pub struct TurnExecutor {
    /// Sub-agent the segments declare; `None` for the main line.
    pub subagent_id: Option<String>,
    /// Heads of the main line: the root-to-leaf path of the executor's
    /// segment forest with the most exchanges, in path order.
    pub main: Vec<String>,
    /// Heads of the other segments, by start.
    pub side: Vec<String>,
    /// Number of exchanges in the executor's segments.
    pub exchange_count: usize,
    /// Earliest start of the executor's segments.
    pub start: DateTime<Utc>,
}

/// One executor's segments, as indices into the session's segments.
struct Executor {
    subagent_id: Option<String>,
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
            executor
                .main
                .iter()
                .find_map(|&segment| segments[segment].label.clone())
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
                    }
                })
                .collect(),
        }
    }

    /// The executors of one turn's segments, each split into its main line
    /// and side segments.
    fn executors(&self, members: &[usize]) -> Vec<Executor> {
        let mut groups: Vec<(Option<&str>, Vec<usize>)> = Vec::new();
        for &segment in members {
            let subagent = self.segments[segment].subagent_id.as_deref();
            match groups.iter_mut().find(|(id, _)| *id == subagent) {
                Some((_, group)) => group.push(segment),
                None => groups.push((subagent, vec![segment])),
            }
        }
        let by_start = |segments: &mut Vec<usize>| {
            segments.sort_by_key(|&segment| (self.segments[segment].start, segment));
        };
        for (_, group) in &mut groups {
            by_start(group);
        }
        groups.sort_by_key(|(subagent, group)| {
            (
                subagent.is_some(),
                group.first().map(|&segment| self.segments[segment].start),
            )
        });
        groups
            .into_iter()
            .map(|(subagent, group)| {
                let main = self.main_line(&group);
                let side = group
                    .iter()
                    .copied()
                    .filter(|segment| !main.contains(segment))
                    .collect();
                Executor {
                    subagent_id: subagent.map(str::to_owned),
                    main,
                    side,
                }
            })
            .collect()
    }

    /// The root-to-leaf path with the most exchanges in the forest that the
    /// `parent` links within `group` form; of equal paths, the one whose
    /// segments start earliest, compared from the root. `group` is ordered
    /// by start.
    fn main_line(&self, group: &[usize]) -> Vec<usize> {
        let parent_of = |segment: usize| {
            self.segmentation.segments[segment]
                .parent
                .filter(|parent| group.contains(parent))
        };
        let mut children: HashMap<usize, Vec<usize>> = HashMap::new();
        for &segment in group {
            if let Some(parent) = parent_of(segment) {
                children.entry(parent).or_default().push(segment);
            }
        }
        // Children come after their parent in segment order, so walking the
        // group backwards by segment index sees every child first.
        let mut order = group.to_vec();
        order.sort_unstable();
        let mut best: HashMap<usize, (usize, Option<usize>)> = HashMap::new();
        for &segment in order.iter().rev() {
            let mut chosen: Option<(usize, usize)> = None;
            for &child in children.get(&segment).into_iter().flatten() {
                let weight = best.get(&child).map_or(0, |(weight, _)| *weight);
                if chosen.is_none_or(|(most, _)| weight > most) {
                    chosen = Some((weight, child));
                }
            }
            let own = self.segments[segment].exchange_count;
            let weight = own.saturating_add(chosen.map_or(0, |(weight, _)| weight));
            best.insert(segment, (weight, chosen.map(|(_, child)| child)));
        }
        let mut root: Option<(usize, usize)> = None;
        for &segment in group {
            if parent_of(segment).is_some() {
                continue;
            }
            let weight = best.get(&segment).map_or(0, |(weight, _)| *weight);
            if root.is_none_or(|(most, _)| weight > most) {
                root = Some((weight, segment));
            }
        }
        let mut path = Vec::new();
        let mut next = root.map(|(_, segment)| segment);
        while let Some(segment) = next {
            path.push(segment);
            next = best.get(&segment).and_then(|(_, child)| *child);
        }
        path
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
                        (
                            None,
                            strings(&["m1", "m3"]),
                            strings(&["search", "recap"]),
                            6
                        ),
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
}
