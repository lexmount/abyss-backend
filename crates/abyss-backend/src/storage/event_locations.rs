//! Attaches derived positions to a response page using one structure lookup
//! per session. The storage caller owns the read snapshot and owner scope.

use std::{collections::HashMap, sync::Arc};

use uuid::Uuid;

use crate::{
    error::AppError,
    search::SessionSearchResponse,
    usage::{UsageEventResponse, streams::SessionStructure},
};

pub(super) fn locate_events<F>(
    events: &mut [UsageEventResponse],
    mut load: F,
) -> Result<(), AppError>
where
    F: FnMut(Uuid) -> Result<Arc<SessionStructure>, AppError>,
{
    let mut structures = HashMap::new();
    for event in events {
        let structure = match structures.entry(event.session_pk) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(load(event.session_pk)?)
            }
        };
        event.location = structure.event_position(event.id);
    }
    Ok(())
}

pub(super) fn locate_search_matches<F>(
    response: &mut SessionSearchResponse,
    mut load: F,
) -> Result<(), AppError>
where
    F: FnMut(Uuid) -> Result<Arc<SessionStructure>, AppError>,
{
    for session in &mut response.items {
        if session.matches.is_empty() {
            continue;
        }
        let structure = load(session.session_pk)?;
        for hit in &mut session.matches {
            hit.location = structure.event_position(hit.event_pk);
        }
    }
    Ok(())
}
