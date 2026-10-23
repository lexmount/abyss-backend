//! Segments: runs of exchanges along one stream, cut at roots, turn changes,
//! and fork points.

use serde::Serialize;

use super::{
    base::{BaseRule, Exchange, NoBaseReason},
    turns::Turns,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Why a segment begins.
pub enum SegmentKind {
    /// Its first exchange has no base.
    Root,
    /// Its first exchange belongs to a turn other than its base's: one
    /// stream moves on to the next user input. The base, the only exchange
    /// that continues it, is not a fork point.
    Turn,
    /// Its first exchange continues a fork point; every branch is a new stream.
    Branch,
}

pub(super) struct Segment {
    pub(super) kind: SegmentKind,
    pub(super) parent: Option<usize>,
    pub(super) exchanges: Vec<usize>,
}

pub(super) struct Segmentation {
    pub(super) segments: Vec<Segment>,
    /// Segment of each exchange.
    pub(super) segment_of: Vec<usize>,
}

impl Segmentation {
    /// Walks the exchanges by request time. A segment begins at an exchange
    /// without a base, at one whose base is a fork point, the base of more
    /// than one exchange, and at one that belongs to a turn other than its
    /// base's; any other exchange joins its base's segment.
    pub(super) fn cut(
        exchanges: &[Exchange],
        bases: &[Result<(usize, BaseRule), NoBaseReason>],
        turns: &Turns,
    ) -> Self {
        let mut children = vec![0_usize; exchanges.len()];
        for &(base, _) in bases.iter().flatten() {
            children[base] = children[base].saturating_add(1);
        }
        let mut segments: Vec<Segment> = Vec::new();
        let mut placed: Vec<Option<usize>> = vec![None; exchanges.len()];
        for index in 0..exchanges.len() {
            let base = bases[index].ok().map(|(base, _)| base);
            let base_segment = base.and_then(|base| placed[base]).zip(base);
            let new_turn =
                |base: usize| turns.of[index].is_some() && turns.of[index] != turns.of[base];
            let segment = match base_segment {
                Some((segment, base)) if children[base] <= 1 && !new_turn(base) => segment,
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
