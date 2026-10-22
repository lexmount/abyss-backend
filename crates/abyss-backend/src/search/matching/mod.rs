//! Literal substring matching every storage backend shares: folding, the
//! pieces of an event, candidate expressions, occurrences, fragments, and
//! the order and bounds of results.
//!
//! The reference model is to fold every piece of every event and look for
//! the folded query in it. Backends may narrow the texts they verify, but a
//! hit is always a piece whose folded text holds the folded query, verified
//! here, so every backend returns the reference model's hits.

pub mod fold;
pub mod fragment;
pub mod in_session;
pub mod pattern;
pub mod pieces;
pub mod ranking;
