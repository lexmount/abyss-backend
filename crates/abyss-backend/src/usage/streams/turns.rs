//! Turns of one session: the user input each exchange works for, from the
//! turn the client declares and, in a session without any declaration, from
//! the requests that open a turn.

use std::collections::HashMap;

use super::base::{BaseRule, Exchange, NoBaseReason};

/// The turn of every exchange of one session.
pub(super) struct Turns {
    /// Index into `ids` of each exchange's turn; `None` when the exchange
    /// belongs to no turn.
    pub(super) of: Vec<Option<usize>>,
    /// Identity of each turn in the order turns first appear: the declared
    /// value, or the id of the exchange that opened it.
    pub(super) ids: Vec<String>,
}

impl Turns {
    /// Assigns each exchange, in session order, its turn.
    ///
    /// When any exchange of the session declares a turn, an exchange with a
    /// declaration belongs to the turn it declares, and one without opens no
    /// turn. When none does, an exchange whose request opens a turn and
    /// continues a context (holds model output or names the response it
    /// continues) opens one named by the exchange's id; one that continues
    /// none opens a turn only when no turn began before it. Any other
    /// exchange takes the turn of its base; without a base, or with a base
    /// later in session order, it joins the turn that most recently began
    /// before it, and belongs to no turn when no turn began before it.
    pub(super) fn assign(
        exchanges: &[Exchange],
        bases: &[Result<(usize, BaseRule), NoBaseReason>],
    ) -> Self {
        let declared = exchanges.iter().any(|exchange| exchange.turn_id.is_some());
        let mut ids: Vec<String> = Vec::new();
        let mut known: HashMap<&str, usize> = HashMap::new();
        let mut of: Vec<Option<usize>> = Vec::with_capacity(exchanges.len());
        for (index, exchange) in exchanges.iter().enumerate() {
            let opened = if declared {
                exchange.turn_id.as_deref()
            } else {
                // A request that continues no context, holding no model output
                // and naming no response, can be a sub-agent's task or a
                // client's own request as well as the user's words: it opens a
                // turn only before any turn began.
                (exchange.starts_turn() && (exchange.continues_context() || ids.is_empty()))
                    .then_some(exchange.id.as_str())
            };
            let turn = match opened {
                Some(id) => Some(*known.entry(id).or_insert_with(|| {
                    ids.push(id.to_owned());
                    ids.len().saturating_sub(1)
                })),
                None => match bases[index] {
                    Ok((base, _)) if base < index => of[base],
                    _ => ids.len().checked_sub(1),
                },
            };
            of.push(turn);
        }
        Self { of, ids }
    }

    /// One-based number of a turn, by first appearance in the session.
    pub(super) fn number(turn: usize) -> i64 {
        i64::try_from(turn).map_or(i64::MAX, |turn| turn.saturating_add(1))
    }
}
