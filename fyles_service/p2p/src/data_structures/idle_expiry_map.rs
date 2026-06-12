#![allow(dead_code)]
use hashbrown::{
    hash_map::{Entry, OccupiedEntry, VacantEntry},
    HashMap,
};
use std::hash::Hash;

#[cfg(test)]
use tokio::time::Instant;

#[cfg(not(test))]
use std::time::Instant;

use std::time::Duration;


#[derive(Debug)]
struct TimedEntry<V> {
    value: V,
    last_touched: Instant,
}

impl<V> TimedEntry<V> {
    fn new(value: V, now: Instant) -> Self {
        Self {
            value,
            last_touched: now,
        }
    }

    fn touch(&mut self, now: Instant) {
        self.last_touched = now;
    }

    fn is_expired(&self, idle_ttl: Duration, now: Instant) -> bool {
        now.duration_since(self.last_touched) >= idle_ttl
    }
}

pub struct IdleExpiryMap<K, V, F: FnMut(V)> {
    entries: HashMap<K, TimedEntry<V>>,
    idle_ttl: Duration,
    cleanup_interval: Duration,
    last_cleanup: Instant,
    on_expire: F,
}

impl<K: std::fmt::Debug, V: std::fmt::Debug, F: FnMut(V)> std::fmt::Debug
    for IdleExpiryMap<K, V, F>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdleExpiryMap")
            .field("entries", &self.entries)
            .field("idle_ttl", &self.idle_ttl)
            .field("cleanup_interval", &self.cleanup_interval)
            .field("last_cleanup", &self.last_cleanup)
            .finish()
    }
}

impl<K, V, F> IdleExpiryMap<K, V, F>
where
    K: Eq + Hash + Clone,
    F: FnMut(V) + Clone,
{
    pub fn new(idle_ttl: Duration, cleanup_interval: Duration, on_expire: F) -> Self {
        let now = Instant::now();
        Self {
            entries: HashMap::new(),
            idle_ttl,
            cleanup_interval,
            last_cleanup: now,
            on_expire,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let now = Instant::now();
        self.cleanup_if_due(now);

        self.entries
            .insert(key, TimedEntry::new(value, now))
            .map(|entry| entry.value)
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        self.get_mut(key).map(|it| &*it)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let now = Instant::now();
        self.cleanup_if_due(now);

        let entry = self.entries.get_mut(key);
        let (expired, entry) = match entry {
            Some(entry) => (entry.is_expired(self.idle_ttl, now), entry),
            None => return None,
        };

        if expired {
            return None;
        }

        entry.touch(now);
        Some(&mut entry.value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.entries.remove(key).and_then(|entry| {
            if entry.is_expired(self.idle_ttl, Instant::now()) {
                None
            } else {
                Some(entry.value)
            }
        })
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.last_cleanup = Instant::now();
    }

    pub fn entry(&mut self, key: K) -> IdleEntry<'_, K, V, F> {
        let now = Instant::now();
        self.cleanup_if_due(now);

        let mut entry = self.entries.entry(key);

        let exists_and_expired = match entry {
            Entry::Occupied(ref occupied_entry) => {
                occupied_entry.get().is_expired(self.idle_ttl, now)
            }
            Entry::Vacant(vacant_entry) => {
                return IdleEntry::Vacant(VacantIdleEntry {
                    entry: vacant_entry,
                });
            }
        };

        if exists_and_expired {
            entry = entry.and_replace_entry_with(|_, v| {
                (self.on_expire)(v.value);
                None
            });
        }

        match entry {
            Entry::Occupied(entry) => IdleEntry::Occupied(OccupiedIdleEntry { entry, timeout: self.idle_ttl, on_expire: self.on_expire.clone() }),
            Entry::Vacant(entry) => IdleEntry::Vacant(VacantIdleEntry { entry }),
        }
    }

    fn cleanup_if_due(&mut self, now: Instant) {
        if now.duration_since(self.last_cleanup) < self.cleanup_interval {
            return;
        }

        self.last_cleanup = now;
        let idle_ttl = self.idle_ttl;


        self.entries
            .extract_if(|_, v| v.is_expired(idle_ttl, now))
            .for_each(|(_, extracted)| {
                (self.on_expire)(extracted.value);
            });
    }
}

pub enum IdleEntry<'a, K, V, F: FnMut(V)>
where
    K: Eq + Hash,
{
    Occupied(OccupiedIdleEntry<'a, K, V, F>),
    Vacant(VacantIdleEntry<'a, K, V>),
}

pub struct OccupiedIdleEntry<'a, K, V, F: FnMut(V)>
where
    K: Eq + Hash,
{
    entry: OccupiedEntry<'a, K, TimedEntry<V>>,
    timeout: Duration,
    on_expire: F,

}

pub struct VacantIdleEntry<'a, K, V>
where
    K: Eq + Hash,
{
    entry: VacantEntry<'a, K, TimedEntry<V>>,
}

impl<'a, K, V, F> OccupiedIdleEntry<'a, K, V, F>
where
    K: Eq + Hash,
    F: FnMut(V)
{
    pub fn get(&mut self) -> Option<&V> {
        let now = Instant::now();
        if now.duration_since(self.entry.get().last_touched) < self.timeout {
            self.entry.get_mut().touch(now);
            Some(&self.entry.get().value)
        } else {
            None
        }
    }

    pub fn get_mut(&mut self) -> Option<&mut V> {
        let now = Instant::now();
        if now.duration_since(self.entry.get().last_touched) < self.timeout {
            let entry = self.entry.get_mut();
            entry.touch(now);
            Some(&mut entry.value)
        } else {
            None
        }
    }

    pub fn into_mut(mut self) -> Option<&'a mut V> {
        let now = Instant::now();
        if now.duration_since(self.entry.get().last_touched) < self.timeout {
            self.entry.get_mut().touch(now);
            Some(&mut self.entry.into_mut().value)
        } else {
            let removed = self.entry.remove();
            (self.on_expire)(removed.value);
            None
        }
    }

    pub fn into_mut_or_insert(mut self, default: impl FnOnce() -> V) -> &'a mut V {
        let now = Instant::now();
        if now.duration_since(self.entry.get().last_touched) < self.timeout {
            self.entry.get_mut().touch(now);
            &mut self.entry.into_mut().value
        } else {
            // let mut inserted = self.entry.insert(TimedEntry { value: default, last_touched: Instant::now() });
            // &mut inserted.value
            let old = self.entry.insert(TimedEntry::new(default(), now));
            (self.on_expire)(old.value);
            &mut self.entry.into_mut().value
        }
    }

    #[must_use = "The old value is returned, which means the destructor cannot run on it. You need to take care to correctly destruct the old value"]
    pub fn insert(&mut self, value: V) -> V {
        let old = std::mem::replace(self.entry.get_mut(), TimedEntry::new(value, Instant::now()));
        old.value
    }

    #[must_use = "The removed value is returned, which means the destructor cannot run on it. You need to take care to correctly destruct the old value or use `destruct` instead"]
    pub fn remove(self) -> V {
        self.entry.remove().value
    }

    pub fn destruct(mut self) {
        (self.on_expire)(self.entry.remove().value);
    }
}

impl<'a, K, V> VacantIdleEntry<'a, K, V>
where
    K: Eq + Hash,
{
    pub fn insert(self, value: V) -> &'a mut V {
        &mut self.entry.insert(TimedEntry::new(value, Instant::now())).value
    }
}

impl<'a, K, V, F: FnMut(V)> IdleEntry<'a, K, V, F>
where
    K: Eq + Hash,
{
    pub fn insert(self, value: V) -> Option<V> {
        match self {
            IdleEntry::Occupied(mut entry) => Some(entry.insert(value)),
            IdleEntry::Vacant(entry) => {
                entry.insert(value);
                None
            }
        }
    }

    pub fn or_insert(self, default: impl FnOnce() -> V) -> &'a mut V {
        match self {
            IdleEntry::Occupied(entry) => entry.into_mut_or_insert(default),
            IdleEntry::Vacant(entry) => entry.insert(default()),
        }
    }

    pub fn or_insert_with(self, default: impl FnOnce() -> V) -> &'a mut V {
        match self {
            IdleEntry::Occupied(entry) => entry.into_mut_or_insert(default),
            IdleEntry::Vacant(entry) => entry.insert(default()),
        }
    }

    pub fn and_modify(self, f: impl FnOnce(&mut V)) -> Self {
        match self {
            IdleEntry::Occupied(mut entry) => {
                if let Some(reference) = entry.get_mut() {
                    f(reference);
                    IdleEntry::Occupied(entry)
                } else {
                    let vacant = entry.entry.replace_entry_with(|_, _| None);
                    if let Entry::Vacant(vacant) = vacant {
                        IdleEntry::Vacant(VacantIdleEntry { entry: vacant })
                    } else {
                        unreachable!("Just vacated the entry");
                    }
                }
            }
            idle => idle,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    use tokio::time::advance;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn insert_then_get_returns_value() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});

        map.insert(1, "hello".to_string());

        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("hello"));
    }

    #[test]
    fn get_mut_allows_mutation() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});

        map.insert(1, "hello".to_string());

        map.get_mut(&1).unwrap().push_str(" world");

        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("hello world"));
    }

    #[tokio::test(start_paused = true)]
    async fn get_touches_entry_and_extends_lifetime() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(100), ms(1_000), |_| {});

        map.insert(1, "a".to_string());

        advance(ms(60)).await;
        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("a"));

        advance(ms(60)).await;
        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("a"));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_entry_is_hidden_on_get_even_before_cleanup() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(50), ms(1_000), |_| {});

        map.insert(1, "a".to_string());

        advance(ms(60)).await;

        assert_eq!(map.get(&1), None);
        assert!(matches!(map.get(&1), None));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_entry_remains_stored_until_cleanup_runs() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(50), ms(1_000), |_| {});

        map.insert(1, "a".to_string());

        advance(ms(60)).await;

        assert!(map.get(&1).is_none());
        assert!(
            map.entries.contains_key(&1),
            "expired entry should still be present before cleanup"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_removes_expired_entries_once_due() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(50), ms(40), |_| {});

        map.insert(1, "a".to_string());

        assert!(map.entries.contains_key(&1));
        assert!(map.get(&1).is_some());
        advance(ms(60)).await;
        assert!(map.entries.contains_key(&1));
        // this triggers cleanup
        assert!(map.get(&1).is_none());
        assert!(!map.entries.contains_key(&1));

        advance(ms(50)).await;
        map.insert(2, "b".to_string());
    }

    #[test]
    fn remove_returns_previous_value() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});

        map.insert(1, "a".to_string());

        assert_eq!(map.remove(&1), Some("a".to_string()));
        assert!(map.get(&1).is_none());
    }

    #[test]
    fn clear_removes_everything() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});

        map.insert(1, "a".to_string());
        map.insert(2, "b".to_string());

        map.clear();

        assert!(map.get(&1).is_none());
        assert!(map.get(&2).is_none());
        assert!(map.entries.is_empty());
    }

    #[test]
    fn entry_vacant_insert_then_get() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});

        match map.entry(1) {
            IdleEntry::Occupied(_) => panic!("expected vacant entry"),
            IdleEntry::Vacant(entry) => {
                let value = entry.insert("created".to_string());
                value.push_str(" now");
            }
        }

        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("created now"));
    }

    #[test]
    fn entry_occupied_get_and_get_mut_touch_value() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});
        map.insert(1, "a".to_string());

        match map.entry(1) {
            IdleEntry::Occupied(mut entry) => {
                assert_eq!(entry.get().map(|it| it.as_str()), Some("a"));
                entry.get_mut().map(|it| it.push_str("b"));
            }
            IdleEntry::Vacant(_) => panic!("expected occupied entry"),
        }

        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("ab"));
    }

    #[test]
    fn occupied_entry_can_be_read_then_modified_then_read_again_without_being_consumed() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});
        map.insert(1, "a".to_string());

        match map.entry(1) {
            IdleEntry::Occupied(mut entry) => {
                assert_eq!(entry.get().map(|it| it.as_str()), Some("a"));
                entry.get_mut().map(|it| it.push('b'));
                assert_eq!(entry.get().map(|it| it.as_str()), Some("ab"));
            }
            IdleEntry::Vacant(_) => panic!("expected occupied entry"),
        }

        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("ab"));
    }

    #[test]
    fn occupied_entry_into_mut_returns_mutable_reference() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});
        map.insert(1, "a".to_string());

        match map.entry(1) {
            IdleEntry::Occupied(entry) => {
                let value = entry.into_mut().expect("entry should not be expired");
                value.push_str("b");
            }
            IdleEntry::Vacant(_) => panic!("expected occupied entry"),
        }

        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("ab"));
    }

    #[test]
    fn entry_occupied_insert_replaces_and_returns_old_value() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});
        map.insert(1, "old".to_string());

        match map.entry(1) {
            IdleEntry::Occupied(mut entry) => {
                let old = entry.insert("new".to_string());
                assert_eq!(old, "old");
            }
            IdleEntry::Vacant(_) => panic!("expected occupied entry"),
        }

        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("new"));
    }

    #[test]
    fn entry_occupied_remove_removes_value() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});
        map.insert(1, "old".to_string());

        match map.entry(1) {
            IdleEntry::Occupied(entry) => {
                assert_eq!(entry.remove(), "old");
            }
            IdleEntry::Vacant(_) => panic!("expected occupied entry"),
        }

        assert!(map.get(&1).is_none());
    }

    #[test]
    fn idle_entry_insert_on_vacant_returns_none_and_inserts_value() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});

        let old = map.entry(1).insert("new".to_string());

        assert_eq!(old, None);
        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("new"));
    }

    #[test]
    fn idle_entry_insert_on_occupied_returns_old_value_and_replaces_it() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});
        map.insert(1, "old".to_string());

        let old = map.entry(1).insert("new".to_string());

        assert_eq!(old, Some("old".to_string()));
        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("new"));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_entry_insert_on_expired_entry_behaves_like_vacant() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(50), ms(1_000), |_| {});
        map.insert(1, "old".to_string());

        advance(ms(60)).await;

        let old = map.entry(1).insert("new".to_string());

        assert_eq!(old, None);
        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("new"));
    }

    #[test]
    fn or_insert_inserts_when_vacant() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});

        let value = map.entry(1).or_insert(|| "default".to_string());
        value.push_str(" value");

        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("default value"));
    }

    #[test]
    fn or_insert_does_not_replace_when_occupied() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});
        map.insert(1, "existing".to_string());

        let value = map.entry(1).or_insert(|| "default".to_string());
        assert_eq!(value, "existing");

        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("existing"));
    }

    #[test]
    fn or_insert_with_inserts_lazily() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});

        let mut called = false;
        let value = map.entry(1).or_insert_with(|| {
            called = true;
            "computed".to_string()
        });

        assert!(called);
        assert_eq!(value, "computed");
    }

    #[test]
    fn or_insert_with_does_not_run_when_occupied() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});
        map.insert(1, "existing".to_string());

        let mut called = false;
        let value = map.entry(1).or_insert_with(|| {
            called = true;
            "computed".to_string()
        });

        assert!(!called);
        assert_eq!(value, "existing");
    }

    #[test]
    fn and_modify_updates_occupied_entry() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});
        map.insert(1, "a".to_string());

        map.entry(1).and_modify(|value| value.push('b'));

        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("ab"));
    }

    #[test]
    fn and_modify_does_nothing_for_vacant_entry() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(1_000), ms(1_000), |_| {});

        map.entry(1)
            .and_modify(|value: &mut String| value.push('x'));

        assert!(map.get(&1).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn entry_treats_expired_value_as_vacant() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(50), ms(1_000), |_| {});
        map.insert(1, "old".to_string());

        advance(ms(60)).await;

        match map.entry(1) {
            IdleEntry::Occupied(_) => panic!("expired entry should not appear occupied"),
            IdleEntry::Vacant(entry) => {
                entry.insert("new".to_string());
            }
        }

        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("new"));
    }

    #[tokio::test(start_paused = true)]
    async fn cleanup_interval_throttles_vacuuming() {
        let mut map = IdleExpiryMap::<u32, String, _>::new(ms(50), ms(100), |_| {});

        map.insert(1, "a".to_string());

        advance(ms(60)).await;
        assert!(map.get(&1).is_none());
        assert!(map.entries.contains_key(&1));

        advance(ms(30)).await;
        map.insert(2, "b".to_string());
        assert!(
            map.entries.contains_key(&1),
            "cleanup should still be throttled"
        );

        advance(ms(20)).await;
        map.insert(3, "c".to_string());
        assert!(
            !map.entries.contains_key(&1),
            "cleanup should run once interval has elapsed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn on_expire_called_during_cleanup() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let expired = Rc::new(RefCell::new(Vec::new()));
        let expired_clone = expired.clone();

        let mut map = IdleExpiryMap::new(ms(50), ms(40), move |v: String| {
            expired_clone.borrow_mut().push(v);
        });

        map.insert(1, "a".to_string());
        map.insert(2, "b".to_string());

        advance(ms(60)).await;
        // Entries expired but cleanup not yet due (last cleanup was at insert time ~0ms,
        // cleanup_interval is 40ms, we're at 60ms now, so next mutating op triggers cleanup)
        map.insert(3, "c".to_string());

        let mut expired_values = expired.borrow().clone();
        expired_values.sort();
        assert_eq!(expired_values, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn on_expire_called_when_entry_evicts_expired_value() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let expired = Rc::new(RefCell::new(Vec::new()));
        let expired_clone = expired.clone();

        let mut map = IdleExpiryMap::new(ms(50), ms(10_000), move |v: String| {
            expired_clone.borrow_mut().push(v);
        });

        map.insert(1, "old".to_string());

        advance(ms(60)).await;

        // entry() should detect the expired value, remove it, and call on_expire
        map.entry(1).or_insert(|| "new".to_string());

        assert_eq!(*expired.borrow(), vec!["old".to_string()]);
        assert_eq!(map.get(&1).map(|s| s.as_str()), Some("new"));
    }

    #[tokio::test(start_paused = true)]
    async fn on_expire_not_called_for_manual_remove() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let expired = Rc::new(RefCell::new(Vec::new()));
        let expired_clone = expired.clone();

        let mut map = IdleExpiryMap::new(ms(1_000), ms(1_000), move |v: String| {
            expired_clone.borrow_mut().push(v);
        });

        map.insert(1, "a".to_string());
        map.remove(&1);

        assert!(expired.borrow().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn on_expire_not_called_for_clear() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let expired = Rc::new(RefCell::new(Vec::new()));
        let expired_clone = expired.clone();

        let mut map = IdleExpiryMap::new(ms(1_000), ms(1_000), move |v: String| {
            expired_clone.borrow_mut().push(v);
        });

        map.insert(1, "a".to_string());
        map.insert(2, "b".to_string());
        map.clear();

        assert!(expired.borrow().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn on_expire_not_called_for_insert_overwrite() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let expired = Rc::new(RefCell::new(Vec::new()));
        let expired_clone = expired.clone();

        let mut map = IdleExpiryMap::new(ms(1_000), ms(1_000), move |v: String| {
            expired_clone.borrow_mut().push(v);
        });

        map.insert(1, "old".to_string());
        let overwritten = map.insert(1, "new".to_string());

        assert_eq!(overwritten, Some("old".to_string()));
        assert!(expired.borrow().is_empty());
    }
}
