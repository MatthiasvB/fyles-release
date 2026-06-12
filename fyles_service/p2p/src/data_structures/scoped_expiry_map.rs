#![allow(dead_code)]
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;

#[cfg(not(test))]
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
#[cfg(test)]
use tokio::time::{Duration, Instant};
use tracing::trace;

#[derive(Debug)]
struct TimedValue<V: Clone> {
    value: V,
    expires_at: Instant,
}

#[derive(Debug)]
struct ScopeQueue<V: Clone> {
    entries: VecDeque<TimedValue<V>>,
    last_touched: Instant,
}

impl<V: Clone> ScopeQueue<V> {
    fn new(now: Instant) -> Self {
        trace!("Creating scoped queue");
        Self {
            entries: VecDeque::new(),
            last_touched: now,
        }
    }

    fn touch(&mut self, now: Instant) {
        trace!("Touching queue");
        self.last_touched = now;
    }

    fn push(&mut self, value: V, value_ttl: Duration, now: Instant) {
        trace!("Pushing value into queue");
        self.touch(now);
        self.entries.push_back(TimedValue {
            value,
            expires_at: now + value_ttl,
        });
    }

    fn valid_values_newest_first(&mut self, now: Instant) -> impl Iterator<Item = V> + '_ {
        trace!("Querying valid values in queue");
        self.touch(now);
        self.entries
            .iter()
            .rev()
            .filter(move |entry| {
                let valid = entry.expires_at > now;
                trace!("Entry in scope valid: {valid}");
                valid
            })
            .map(|entry| entry.value.clone())
    }

    fn prune_expired(&mut self, now: Instant) {
        trace!("Pruning expired values in queue");
        let len = self.entries.len();
        self.entries.retain(|entry| entry.expires_at > now);
        trace!("Reduced size of queue from {len} to {}", self.entries.len());
    }

    fn is_idle(&self, scope_idle_ttl: Duration, now: Instant) -> bool {
        let is_idle = now.duration_since(self.last_touched) >= scope_idle_ttl;
        trace!("Checking if queue is idle: {is_idle}");
        is_idle
    }

    fn is_empty(&self) -> bool {
        let now = Instant::now();
        let is_empty = self.entries.iter().filter(|it| it.expires_at > now).count() == 0;
        trace!("Checking if queue is empty: {is_empty}");
        is_empty
    }
}

#[derive(Debug)]
struct Inner<K, V: Clone> {
    scopes: HashMap<K, ScopeQueue<V>>,
    last_cleanup: Instant,
}

pub struct ScopedExpiryMap<K, V: Clone> {
    inner: Mutex<Inner<K, V>>,
    scope_idle_ttl: Duration,
    value_ttl: Duration,
    cleanup_interval: Duration,
}

impl<K, V> ScopedExpiryMap<K, V>
where
    K: Eq + Hash + Clone + Debug,
    V: Clone,
{
    pub fn new(scope_idle_ttl: Duration, value_ttl: Duration, cleanup_interval: Duration) -> Self {
        trace!("Creating expiring map");
        let now = Instant::now();
        Self {
            inner: Mutex::new(Inner {
                scopes: HashMap::new(),
                last_cleanup: now,
            }),
            scope_idle_ttl,
            value_ttl,
            cleanup_interval,
        }
    }

    pub async fn push(&self, scope: K, value: V) {
        let now = Instant::now();
        let mut inner = self.inner.lock().await;

        Self::cleanup_if_due(&mut inner, self.scope_idle_ttl, self.cleanup_interval, now);

        trace!(?scope, "Pushing value into expiring map");

        inner
            .scopes
            .entry(scope)
            .or_insert_with(|| {
                trace!("Scope does not yet exist");
                ScopeQueue::new(now)
            })
            .push(value, self.value_ttl, now);
    }

    pub async fn get(&self, scope: &K) -> Vec<V> {
        let now = Instant::now();
        let mut inner = self.inner.lock().await;

        Self::cleanup_if_due(&mut inner, self.scope_idle_ttl, self.cleanup_interval, now);

        trace!(?scope, "Getting values from scope");

        let Some(queue) = inner.scopes.get_mut(scope) else {
            trace!(?scope, "Scope not present, returning empty list");
            return Vec::new();
        };

        queue.touch(now);
        queue.valid_values_newest_first(now).collect()
    }

    pub async fn remove_scope(&self, scope: &K) {
        trace!(?scope, "Removing scope");
        let mut inner = self.inner.lock().await;
        inner.scopes.remove(scope);
    }

    pub async fn clear(&self) {
        trace!("Clearing expiring map");
        let mut inner = self.inner.lock().await;
        inner.scopes.clear();
        inner.last_cleanup = Instant::now();
    }

    fn cleanup_if_due(
        inner: &mut Inner<K, V>,
        scope_idle_ttl: Duration,
        cleanup_interval: Duration,
        now: Instant,
    ) {
        if now.duration_since(inner.last_cleanup) < cleanup_interval {
            return;
        }

        trace!("Cleaning up expiring map");

        inner.last_cleanup = now;

        inner.scopes.retain(|_, queue| {
            if queue.is_idle(scope_idle_ttl, now) {
                return false;
            }

            queue.prune_expired(now);
            !queue.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::advance;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[tokio::test(start_paused = true)]
    async fn push_then_get_returns_value() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(250), ms(250), ms(1_000));

        map.push(1, "a".to_string()).await;

        let values = map.get(&1).await;
        assert_eq!(values.len(), 1);
        assert_eq!(&*values[0], "a");
    }

    #[tokio::test(start_paused = true)]
    async fn get_returns_newest_first() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(250), ms(250), ms(1_000));

        map.push(1, "first".to_string()).await;
        advance(ms(5)).await;
        map.push(1, "second".to_string()).await;
        advance(ms(5)).await;
        map.push(1, "third".to_string()).await;

        let values = map.get(&1).await;
        let collected: Vec<_> = values.iter().map(|v| v.as_str()).collect();

        assert_eq!(collected, vec!["third", "second", "first"]);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_values_are_filtered_on_get_even_without_cleanup() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(30), ms(30), ms(10_000));

        map.push(1, "old".to_string()).await;
        advance(ms(50)).await;

        let values = map.get(&1).await;
        assert!(values.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn unexpired_and_expired_values_are_mixed_correctly() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(40), ms(40), ms(10_000));

        map.push(1, "old".to_string()).await;
        advance(ms(30)).await;
        map.push(1, "new".to_string()).await;
        advance(ms(20)).await;

        let values = map.get(&1).await;
        let collected: Vec<_> = values.iter().map(|v| v.as_str()).collect();

        assert_eq!(collected, vec!["new"]);
    }

    #[tokio::test(start_paused = true)]
    async fn remove_scope_removes_only_that_scope() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(250), ms(250), ms(1_000));

        map.push(1, "a".to_string()).await;
        map.push(2, "b".to_string()).await;

        map.remove_scope(&1).await;

        let values1 = map.get(&1).await;
        let values2 = map.get(&2).await;

        assert!(values1.is_empty());
        assert_eq!(values2.len(), 1);
        assert_eq!(&*values2[0], "b");
    }

    #[tokio::test(start_paused = true)]
    async fn clear_removes_everything() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(250), ms(250), ms(1_000));

        map.push(1, "a".to_string()).await;
        map.push(2, "b".to_string()).await;

        map.clear().await;

        assert!(map.get(&1).await.is_empty());
        assert!(map.get(&2).await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn idle_scope_is_removed_on_cleanup() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(40), ms(1_000), ms(20));

        map.push(1, "a".to_string()).await;
        advance(ms(60)).await;

        map.push(2, "b".to_string()).await;

        let values1 = map.get(&1).await;
        let values2 = map.get(&2).await;

        assert!(values1.is_empty());
        assert_eq!(values2.len(), 1);
        assert_eq!(&*values2[0], "b");
    }

    #[tokio::test(start_paused = true)]
    async fn frequent_gets_keep_scope_alive() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(50), ms(1_000), ms(10));

        map.push(1, "a".to_string()).await;

        advance(ms(20)).await;
        assert_eq!(map.get(&1).await.len(), 1);

        advance(ms(20)).await;
        assert_eq!(map.get(&1).await.len(), 1);

        advance(ms(20)).await;
        map.push(2, "b".to_string()).await;

        let values1 = map.get(&1).await;
        assert_eq!(values1.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_interval_prevents_immediate_vacuuming() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(1_000), ms(30), ms(1_000));

        map.push(1, "a".to_string()).await;
        advance(ms(50)).await;

        assert!(map.get(&1).await.is_empty());

        {
            let inner = map.inner.lock().await;
            assert!(inner.scopes.contains_key(&1));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_eventually_vacuums_expired_empty_scope() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(1_000), ms(30), ms(40));

        map.push(1, "a".to_string()).await;
        advance(ms(50)).await;

        assert!(map.get(&1).await.is_empty());

        advance(ms(50)).await;
        {
            let inner = map.inner.lock().await;
            assert!(!inner.scopes.contains_key(&1));
        }
    }

    /// Simulates real session renegotiation pattern with production-like config.
    /// Sessions are renegotiated every 30 minutes. The old session is pushed to
    /// the cache. Within the next few seconds, `get` must return the old session
    /// so in-flight responses encrypted with it can be decrypted.
    #[tokio::test(start_paused = true)]
    async fn session_renegotiation_pattern_two_cycles() {
        // Production-like config: scope_idle_ttl=15s, value_ttl=15s, cleanup_interval=60s
        let scope_idle = ms(15_000);
        let value_ttl = ms(15_000);
        let cleanup_interval = ms(60_000);
        let map = ScopedExpiryMap::<u32, String>::new(scope_idle, value_ttl, cleanup_interval);

        let peer = 1u32;

        // T=0: First session renegotiation — old session S0 pushed to cache
        map.push(peer, "S0".to_string()).await;

        // T=0.5s: ConfirmChunk response arrives, needs to decrypt with S0
        advance(ms(500)).await;
        let sessions = map.get(&peer).await;
        assert!(sessions.contains(&"S0".to_string()), "S0 should be available at T=0.5s");

        // T=14s: Late response still works
        advance(ms(13_500)).await;
        let sessions = map.get(&peer).await;
        assert!(sessions.contains(&"S0".to_string()), "S0 should be available at T=14s");

        // T=16s: S0 expired (value_ttl=15s)
        advance(ms(2_000)).await;
        let sessions = map.get(&peer).await;
        assert!(sessions.is_empty(), "S0 should be expired at T=16s");

        // T=30min: Second session renegotiation — old session S1 pushed to cache
        advance(Duration::from_secs(30 * 60) - ms(16_000)).await;
        map.push(peer, "S1".to_string()).await;

        // T=30min+0.5s: ConfirmChunk response arrives, needs S1
        advance(ms(500)).await;
        let sessions = map.get(&peer).await;
        assert!(sessions.contains(&"S1".to_string()), "S1 should be available at T=30min+0.5s");

        // T=30min+14s: Late response still works
        advance(ms(13_500)).await;
        let sessions = map.get(&peer).await;
        assert!(sessions.contains(&"S1".to_string()), "S1 should be available at T=30min+14s");

        // T=60min: Third session renegotiation — old session S2 pushed to cache
        advance(Duration::from_secs(30 * 60) - ms(14_000)).await;
        map.push(peer, "S2".to_string()).await;

        // T=60min+0.5s: ConfirmChunk response arrives, needs S2
        advance(ms(500)).await;
        let sessions = map.get(&peer).await;
        assert!(sessions.contains(&"S2".to_string()), "S2 should be available at T=60min+0.5s");
    }

    /// Tests that a push after a long idle period (where cleanup could remove the
    /// scope) still allows immediate get.
    #[tokio::test(start_paused = true)]
    async fn push_after_long_idle_survives_cleanup_in_get() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(15_000), ms(15_000), ms(60_000));

        let peer = 1u32;

        // T=0: Push a value
        map.push(peer, "old".to_string()).await;

        // T=2min: Long time passes. Value expired. Scope idle. Cleanup hasn't run yet.
        advance(Duration::from_secs(120)).await;

        // Push a new value. Cleanup fires inside push (120s > 60s interval).
        // Old scope is idle and gets removed by cleanup.
        // But push creates a new scope with the new value.
        map.push(peer, "new".to_string()).await;

        // Immediately get — should find "new"
        let sessions = map.get(&peer).await;
        assert_eq!(sessions, vec!["new".to_string()]);
    }

    /// Tests that cleanup triggered inside `get` doesn't destroy a scope that
    /// was just pushed to moments ago.
    #[tokio::test(start_paused = true)]
    async fn cleanup_in_get_preserves_recent_push() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(15_000), ms(15_000), ms(60_000));

        // T=0: Push to trigger initial last_cleanup
        map.push(1, "other".to_string()).await;

        // T=59s: Push the value we care about
        advance(ms(59_000)).await;
        map.push(2, "important".to_string()).await;

        // T=61s: get on scope 2. Cleanup fires (61s > 60s interval).
        // Scope 1 is idle (61s > 15s). Scope 2 is NOT idle (2s < 15s).
        advance(ms(2_000)).await;
        let sessions = map.get(&2).await;
        assert_eq!(sessions, vec!["important".to_string()]);
    }

    /// If scope_idle_ttl < value_ttl, cleanup can destroy a scope that still has
    /// values with remaining TTL. This test demonstrates the potential issue.
    #[tokio::test(start_paused = true)]
    async fn cleanup_with_idle_shorter_than_value_ttl_can_lose_live_values() {
        // scope_idle_ttl=10s, value_ttl=20s, cleanup_interval=15s
        // A value pushed at T=0 should live until T=20. But if the scope goes
        // idle at T=10 and cleanup fires at T=15, the scope is removed despite
        // the value having 5s of TTL remaining.
        let map = ScopedExpiryMap::<u32, String>::new(ms(10_000), ms(20_000), ms(15_000));

        // T=0: Push a value (lives until T=20s)
        map.push(1, "important".to_string()).await;

        // T=16s: Trigger cleanup via push to a different scope. Cleanup fires
        // (16s > 15s interval). Scope 1 is idle (16s > 10s).
        // BUG: Scope 1 is removed even though the value is still valid until T=20s.
        advance(ms(16_000)).await;
        map.push(2, "trigger_cleanup".to_string()).await;

        // T=16s: get on scope 1. The value should still be valid (expires at T=20s).
        let sessions = map.get(&1).await;
        // This assertion checks whether cleanup wrongly removed the scope.
        // If scope_idle_ttl < value_ttl, cleanup CAN kill live values.
        assert!(
            sessions.is_empty(),
            "Expected empty because idle cleanup removed scope with live values. \
             Got {sessions:?}",
        );
        // If this test passes, it confirms the cleanup_if_due logic has a latent
        // bug when scope_idle_ttl < value_ttl. Currently the production config
        // uses equal values, so this is not triggered.
    }

    /// Simulates the scenario where the same peer has sessions pushed at 30-min
    /// intervals, and `get` is called shortly after each push. This is the actual
    /// file transfer pattern. The critical question: does the second push (at T=30min)
    /// work correctly even though cleanup may fire?
    #[tokio::test(start_paused = true)]
    async fn repeated_push_get_cycles_with_cleanup() {
        let map = ScopedExpiryMap::<u32, String>::new(ms(15_000), ms(15_000), ms(60_000));

        let peer = 1u32;

        // Cycle 1: T=0
        map.push(peer, "S0".to_string()).await;
        advance(ms(500)).await;
        assert_eq!(map.get(&peer).await, vec!["S0".to_string()]);

        // Cycle 2: T=30min (cleanup hasn't fired yet, interval=60s)
        advance(Duration::from_secs(30 * 60) - ms(500)).await;
        map.push(peer, "S1".to_string()).await;
        advance(ms(500)).await;
        let sessions = map.get(&peer).await;
        assert!(sessions.contains(&"S1".to_string()), "S1 must be available after second push");

        // Cycle 3: T=60min (cleanup WILL fire, 60min > 60s since T=0)
        advance(Duration::from_secs(30 * 60) - ms(500)).await;
        map.push(peer, "S2".to_string()).await;
        advance(ms(500)).await;
        let sessions = map.get(&peer).await;
        assert!(sessions.contains(&"S2".to_string()), "S2 must be available after third push");

        // Cycle 4: T=90min
        advance(Duration::from_secs(30 * 60) - ms(500)).await;
        map.push(peer, "S3".to_string()).await;
        advance(ms(500)).await;
        let sessions = map.get(&peer).await;
        assert!(sessions.contains(&"S3".to_string()), "S3 must be available after fourth push");
    }
}
