//! Shared session structures for synchronous storage reads. The caller reads
//! the revision and event facts from one database snapshot. Per-session locks
//! merge concurrent misses without holding the cache's map lock during I/O.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use uuid::Uuid;

use crate::{
    error::AppError,
    usage::streams::{SessionStructure, StreamEvent},
};

const DEFAULT_CAPACITY: usize = 128;

type SessionKey = (Uuid, Uuid, i64);
type SessionSlot = Arc<Mutex<Option<Arc<SessionStructure>>>>;

struct Entry {
    last_used: u64,
    slot: SessionSlot,
}

#[derive(Default)]
struct State {
    clock: u64,
    entries: HashMap<SessionKey, Entry>,
}

/// At most `capacity` idle session revisions are retained. Active loaders and
/// their waiters pin their slots until they finish; pruning then restores the
/// limit. Structures already returned to callers remain valid after eviction.
pub struct StreamCache {
    capacity: usize,
    state: Mutex<State>,
}

impl Default for StreamCache {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl StreamCache {
    /// With zero capacity, only overlapping calls can share a loaded result.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            state: Mutex::new(State::default()),
        }
    }

    /// Reads and derives the structure only when this exact revision is absent.
    /// A failed load is retryable. Revisions have separate slots, so older
    /// snapshots share their own result without replacing newer snapshots.
    pub fn get_or_load<F>(
        &self,
        owner: Uuid,
        session_pk: Uuid,
        revision: i64,
        load: F,
    ) -> Result<Arc<SessionStructure>, AppError>
    where
        F: FnOnce() -> Result<Vec<StreamEvent>, AppError>,
    {
        let slot = self.slot((owner, session_pk, revision));
        let result = Self::load_slot(&slot, load);
        drop(slot);
        self.prune();
        result
    }

    fn load_slot<F>(slot: &SessionSlot, load: F) -> Result<Arc<SessionStructure>, AppError>
    where
        F: FnOnce() -> Result<Vec<StreamEvent>, AppError>,
    {
        let mut cached = lock(slot);
        if let Some(cached) = cached.as_ref() {
            return Ok(Arc::clone(cached));
        }
        let structure = Arc::new(SessionStructure::derive(load()?));
        *cached = Some(Arc::clone(&structure));
        drop(cached);
        Ok(structure)
    }

    fn slot(&self, key: SessionKey) -> SessionSlot {
        let mut state = lock(&self.state);
        state.clock = state.clock.saturating_add(1);
        let last_used = state.clock;
        let entry = state.entries.entry(key).or_insert_with(|| Entry {
            last_used,
            slot: Arc::new(Mutex::new(None)),
        });
        entry.last_used = last_used;
        let slot = Arc::clone(&entry.slot);
        let evicted = Self::trim(&mut state, self.capacity);
        drop(state);
        // A large evicted structure is freed outside the global map lock.
        drop(evicted);
        slot
    }

    fn prune(&self) {
        let evicted = Self::trim(&mut lock(&self.state), self.capacity);
        drop(evicted);
    }

    fn trim(state: &mut State, capacity: usize) -> Vec<Entry> {
        let mut evicted = Vec::new();
        while state.entries.len() > capacity {
            let candidate = state
                .entries
                .iter()
                .filter(|(_, entry)| Arc::strong_count(&entry.slot) == 1)
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| *key);
            let Some(key) = candidate else {
                break;
            };
            if let Some(entry) = state.entries.remove(&key) {
                evicted.push(entry);
            }
        }
        evicted
    }
}

// Only fully constructed structures are published; a loader panic leaves the
// previous value valid. Recover the lock so subsequent requests can retry.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };

    use super::*;

    const OWNER: Uuid = Uuid::from_u128(1);
    const SESSION: Uuid = Uuid::from_u128(2);

    #[test]
    fn the_same_revision_reuses_one_structure_and_a_new_revision_rebuilds() {
        let cache = StreamCache::default();
        let first = cache
            .get_or_load(OWNER, SESSION, 1, || Ok(Vec::new()))
            .expect("load");
        let repeated = cache
            .get_or_load(OWNER, SESSION, 1, || panic!("cache hit must not load"))
            .expect("hit");
        assert!(Arc::ptr_eq(&first, &repeated));
        let updated = cache
            .get_or_load(OWNER, SESSION, 2, || Ok(Vec::new()))
            .expect("reload");
        assert!(!Arc::ptr_eq(&first, &updated));
        let stale = cache
            .get_or_load(OWNER, SESSION, 1, || Ok(Vec::new()))
            .expect("old snapshot");
        assert!(!Arc::ptr_eq(&stale, &updated));
        let current = cache
            .get_or_load(OWNER, SESSION, 2, || panic!("old snapshot replaced latest"))
            .expect("latest hit");
        assert!(Arc::ptr_eq(&updated, &current));
    }

    #[test]
    fn the_same_session_is_scoped_to_its_owner() {
        let cache = StreamCache::default();
        let first = cache
            .get_or_load(OWNER, SESSION, 1, || Ok(Vec::new()))
            .expect("load");
        let other = cache
            .get_or_load(Uuid::from_u128(3), SESSION, 1, || Ok(Vec::new()))
            .expect("other owner");
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[test]
    fn failed_loads_can_retry_without_discarding_an_older_revision() {
        let cache = StreamCache::default();
        let first = cache
            .get_or_load(OWNER, SESSION, 1, || Ok(Vec::new()))
            .expect("load");
        assert!(
            cache
                .get_or_load(OWNER, SESSION, 2, || Err(AppError::internal(
                    "unavailable".to_owned()
                )))
                .is_err()
        );
        let previous = cache
            .get_or_load(OWNER, SESSION, 1, || panic!("previous result was lost"))
            .expect("previous hit");
        assert!(Arc::ptr_eq(&first, &previous));
        let retried = cache
            .get_or_load(OWNER, SESSION, 2, || Ok(Vec::new()))
            .expect("retry");
        assert!(!Arc::ptr_eq(&first, &retried));
    }

    #[test]
    fn concurrent_misses_for_one_revision_load_only_once() {
        let cache = StreamCache::default();
        let loads = AtomicUsize::new(0);
        let barrier = Barrier::new(8);
        let structures = thread::scope(|scope| {
            let threads = std::iter::repeat_with(|| {
                scope.spawn(|| {
                    barrier.wait();
                    cache
                        .get_or_load(OWNER, SESSION, 1, || {
                            loads.fetch_add(1, Ordering::SeqCst);
                            Ok(Vec::new())
                        })
                        .expect("load shared session")
                })
            })
            .take(8)
            .collect::<Vec<_>>();
            threads
                .into_iter()
                .map(|thread| thread.join().expect("worker"))
                .collect::<Vec<_>>()
        });
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert!(
            structures
                .iter()
                .all(|value| Arc::ptr_eq(value, &structures[0]))
        );
    }

    #[test]
    fn independent_sessions_load_concurrently_and_are_pruned_afterwards() {
        let cache = StreamCache::new(1);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_first_tx, release_first_rx) = mpsc::channel();
        let (release_second_tx, release_second_rx) = mpsc::channel();
        thread::scope(|scope| {
            let cache = &cache;
            let first_started = started_tx.clone();
            let first = scope.spawn(move || {
                cache.get_or_load(OWNER, SESSION, 1, || {
                    first_started.send(()).expect("started");
                    release_first_rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("release first");
                    Ok(Vec::new())
                })
            });
            let second = scope.spawn(move || {
                cache.get_or_load(OWNER, Uuid::from_u128(3), 1, || {
                    started_tx.send(()).expect("started");
                    release_second_rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("release second");
                    Ok(Vec::new())
                })
            });
            for _ in 0..2_usize {
                started_rx
                    .recv_timeout(Duration::from_secs(2))
                    .expect("both loads started");
            }
            release_first_tx.send(()).expect("release first");
            release_second_tx.send(()).expect("release second");
            first.join().expect("first worker").expect("first load");
            second.join().expect("second worker").expect("second load");
        });
        assert_eq!(lock(&cache.state).entries.len(), 1);
    }

    #[test]
    fn capacity_evicts_the_least_recently_used_session() {
        let cache = StreamCache::new(2);
        let a = Uuid::from_u128(10);
        let b = Uuid::from_u128(11);
        let c = Uuid::from_u128(12);
        let first = cache
            .get_or_load(OWNER, a, 1, || Ok(Vec::new()))
            .expect("a");
        let oldest = cache
            .get_or_load(OWNER, b, 1, || Ok(Vec::new()))
            .expect("b");
        let touched = cache
            .get_or_load(OWNER, a, 1, || panic!("a must be cached"))
            .expect("a hit");
        assert!(Arc::ptr_eq(&first, &touched));
        cache
            .get_or_load(OWNER, c, 1, || Ok(Vec::new()))
            .expect("c");
        assert_eq!(lock(&cache.state).entries.len(), 2);
        assert!(lock(&cache.state).entries.contains_key(&(OWNER, a, 1)));
        let reloaded = cache
            .get_or_load(OWNER, b, 1, || Ok(Vec::new()))
            .expect("b evicted");
        assert!(!Arc::ptr_eq(&oldest, &reloaded));
        assert_eq!(lock(&cache.state).entries.len(), 2);
    }
}
