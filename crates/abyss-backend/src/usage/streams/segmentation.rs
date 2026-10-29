//! Segments: the units the session page reads. The exchanges of a turn are
//! grouped by the sub-agent they declare, or, in a session without any
//! declaration, by the line of context they continue; each group's main line
//! is one segment and each of its side exchanges is a segment of its own.

use std::collections::{HashMap, HashSet};

use serde::Serialize;

use super::{
    base::{BaseRule, Exchange, NoBaseReason},
    turns::Turns,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
/// How a segment's first exchange continues its context.
pub enum SegmentKind {
    /// Its first exchange has no base, or a base that is not earlier.
    Root,
    /// Its first exchange continues an exchange that no other exchange
    /// continues.
    Turn,
    /// Its first exchange continues a fork point, the base of more than one
    /// exchange.
    Branch,
}

/// Which group of its turn an exchange belongs to.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum Group {
    /// The sub-agent the exchange declares; `None` for the main agent.
    Declared(Option<String>),
    /// The line of context, named by its first exchange.
    Line(usize),
}

pub(super) struct Segment {
    pub(super) kind: SegmentKind,
    pub(super) parent: Option<usize>,
    pub(super) exchanges: Vec<usize>,
    pub(super) group: Group,
    /// Whether the segment is a single side exchange.
    pub(super) side: bool,
}

pub(super) struct Segmentation {
    /// Segments by their first exchange in session order.
    pub(super) segments: Vec<Segment>,
    /// Segment of each exchange.
    pub(super) segment_of: Vec<usize>,
}

impl Segmentation {
    /// Groups each turn's exchanges, splits each group into its main line and
    /// side exchanges, and makes the main line one segment and every side
    /// exchange a segment of its own.
    ///
    /// An exchange is continued when another exchange has it as base. A side
    /// exchange is not continued, and either another exchange with the same
    /// base is, or it has no base and its group continues something else in
    /// the turn; a group whose exchanges in a turn would all be side keeps
    /// them as its main line. Everything else is the main line.
    pub(super) fn cut(
        exchanges: &[Exchange],
        bases: &[Result<(usize, BaseRule), NoBaseReason>],
        turns: &Turns,
    ) -> Self {
        let count = exchanges.len();
        let base = |index: usize| {
            bases[index]
                .ok()
                .map(|(base, _)| base)
                .filter(|&base| base != index)
        };
        // A base later than its exchange, or the exchange itself, places it
        // as if it had none.
        let earlier = |index: usize| base(index).filter(|&base| base < index);
        let mut children = vec![0_usize; count];
        let mut continued = vec![false; count];
        for index in 0..count {
            if let Some(base) = base(index) {
                children[base] = children[base].saturating_add(1);
                continued[base] = true;
            }
        }
        let mut continued_children = vec![0_usize; count];
        for (index, &is_continued) in continued.iter().enumerate() {
            if let Some(base) = base(index).filter(|_| is_continued) {
                continued_children[base] = continued_children[base].saturating_add(1);
            }
        }
        let declared = exchanges.iter().any(|exchange| {
            exchange.turn_id.is_some()
                || exchange.subagent_id.is_some()
                || exchange.parent_subagent_id.is_some()
        });
        let mut groups: Vec<Group> = Vec::with_capacity(count);
        for (index, exchange) in exchanges.iter().enumerate() {
            let group = if declared {
                Group::Declared(exchange.subagent_id.clone())
            } else {
                match earlier(index) {
                    Some(base) if !(continued[index] && continued_children[base] > 1) => {
                        groups[base].clone()
                    }
                    _ => Group::Line(index),
                }
            };
            groups.push(group);
        }
        let key = |index: usize| (turns.of[index], groups[index].clone());
        let has_continued: HashSet<_> = (0..count)
            .filter(|&index| continued[index])
            .map(key)
            .collect();
        let side = |index: usize| {
            !continued[index]
                && base(index).map_or_else(
                    || has_continued.contains(&key(index)),
                    // Not continued itself, so any continued child is another.
                    |base| continued_children[base] > 0,
                )
        };
        let mut sides: Vec<bool> = (0..count).map(side).collect();
        // A group with nothing but side exchanges in a turn has no main line
        // for them to stand beside: they are its main line.
        let with_main: HashSet<_> = (0..count).filter(|&index| !sides[index]).map(key).collect();
        for (index, side) in sides.iter_mut().enumerate() {
            if !with_main.contains(&key(index)) {
                *side = false;
            }
        }
        let mut segments: Vec<Segment> = Vec::new();
        let mut segment_of = vec![0_usize; count];
        let mut main_of: HashMap<(Option<usize>, Group), usize> = HashMap::new();
        for (index, &is_side) in sides.iter().enumerate() {
            let segment = if is_side {
                None
            } else {
                main_of.get(&key(index)).copied()
            };
            let segment = segment.unwrap_or_else(|| {
                let kind = match earlier(index) {
                    None => SegmentKind::Root,
                    Some(base) if children[base] > 1 => SegmentKind::Branch,
                    Some(_) => SegmentKind::Turn,
                };
                segments.push(Segment {
                    kind,
                    parent: earlier(index).map(|base| segment_of[base]),
                    exchanges: Vec::new(),
                    group: groups[index].clone(),
                    side: is_side,
                });
                let segment = segments.len().saturating_sub(1);
                if !is_side {
                    main_of.insert(key(index), segment);
                }
                segment
            });
            segments[segment].exchanges.push(index);
            segment_of[index] = segment;
        }
        Self {
            segments,
            segment_of,
        }
    }
}
