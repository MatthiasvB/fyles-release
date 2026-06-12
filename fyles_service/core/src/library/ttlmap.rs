use std::hash::Hash;

use expiringmap::{ExpiringMap, ExpiringSet};

pub struct TtlMap<K, V> {
    map: ExpiringMap<K, V>,
    ttl: std::time::Duration,
}

#[allow(unused)]
impl<K, V> TtlMap<K, V>
where
    K: Eq + PartialEq + Hash,
{
    pub fn new(ttl: std::time::Duration) -> Self {
        Self {
            map: ExpiringMap::new(),
            ttl,
        }
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.map.vacuum_if_needed();
        self.map.insert(key, value, self.ttl).and_then(|it| {
            if it.expired() {
                None
            } else {
                Some(it.value())
            }
        })
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.map.get(key)
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.map.get_mut(key)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.map.remove_entry(key).map(|(_, v)| v)
    }

    pub fn clear(&mut self) {
        self.map = Self::new(self.ttl).map;
    }
}

#[allow(unused)]
pub struct TtlSet<V> {
    set: ExpiringSet<V>,
    ttl: std::time::Duration,
}

#[allow(unused)]
impl<V> TtlSet<V>
where
    V: Eq + PartialEq + Hash,
{
    pub fn new(ttl: std::time::Duration) -> Self {
        Self {
            set: ExpiringSet::new(),
            ttl,
        }
    }

    pub fn insert(&mut self, value: V) {
        self.set.vacuum_if_needed();
        self.set.insert(value, self.ttl);
    }

    pub fn contains(&self, value: &V) -> bool {
        self.set.contains(value)
    }

    pub fn remove(&mut self, value: &V) -> bool {
        self.set.remove(value)
    }
}
