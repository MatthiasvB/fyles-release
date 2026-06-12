use std::fmt::Debug;

use tracing::warn as debug;

#[allow(unused)]
pub struct TrackMap<K, V> {
    map: std::collections::HashMap<K, V>,
}

pub enum TrackEntry<'a, K: 'a, V: 'a> {
    Occupied(OccupiedTrackEntry<'a, K, V>),

    Vacant(VacantTrackEntry<'a, K, V>),
}

pub struct OccupiedTrackEntry<'a, K: 'a, V: 'a> {
    base: std::collections::hash_map::OccupiedEntry<'a, K, V>,
}

pub struct VacantTrackEntry<'a, K: 'a, V: 'a> {
    base: std::collections::hash_map::VacantEntry<'a, K, V>,
}

#[allow(unused)]
impl<K: std::hash::Hash + Eq + Debug, V: Debug> TrackMap<K, V> {
    pub fn new() -> Self {
        debug!("Creating new TrackMap");
        Self {
            map: std::collections::HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        debug!("Inserting value into TrackMap: key: {key:?}, value: {value:?}");
        self.map.insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        let value = self.map.get(key);
        debug!("Getting value from TrackMap: key: {key:?} -> {value:?}");
        value
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let value = self.map.get_mut(key);
        debug!("Getting mutable value from TrackMap: key: {key:?} -> {value:?}");
        value
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        debug!("Removing value from TrackMap: key: {key:?}");
        self.map.remove(key)
    }

    pub fn entry(&mut self, key: K) -> TrackEntry<'_, K, V> {
        debug!("Accessing entry in TrackMap: key: {key:?}");
        match self.map.entry(key) {
            std::collections::hash_map::Entry::Occupied(occ) => {
                TrackEntry::Occupied(OccupiedTrackEntry { base: occ })
            }
            std::collections::hash_map::Entry::Vacant(vac) => {
                TrackEntry::Vacant(VacantTrackEntry { base: vac })
            }
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        debug!("Creating iterator over TrackMap");
        self.map.iter()
    }
}

impl<K: std::hash::Hash + Eq + Debug, V: Debug> Default for TrackMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(unused)]
impl<'a, K: std::hash::Hash + Eq + Debug, V: Debug> TrackEntry<'a, K, V> {
    pub fn or_insert(self, value: V) -> &'a mut V {
        debug!("Inserting or accessing entry in TrackMap with default value: value: {value:?}");
        match self {
            TrackEntry::Occupied(occ) => occ.into_mut(),
            TrackEntry::Vacant(vac) => vac.insert(value),
        }
    }

    pub fn insert_entry(self, value: V) -> OccupiedTrackEntry<'a, K, V> {
        debug!("Inserting value into TrackMap entry: value: {value:?}");
        match self {
            TrackEntry::Occupied(mut occ) => {
                occ.insert(value);
                occ
            }
            TrackEntry::Vacant(vac) => vac.insert_entry(value),
        }
    }
}

#[allow(unused)]
impl<'a, K: std::hash::Hash + Eq + Debug, V: Default + Debug> TrackEntry<'a, K, V> {
    pub fn or_default(self) -> &'a mut V {
        debug!("Inserting or accessing entry in TrackMap with default value");
        match self {
            TrackEntry::Occupied(occ) => occ.into_mut(),
            TrackEntry::Vacant(vac) => vac.insert(V::default()),
        }
    }
}

impl<'a, K: std::hash::Hash + Eq + Debug, V: Debug> OccupiedTrackEntry<'a, K, V> {
    pub fn into_mut(self) -> &'a mut V {
        debug!("Accessing mutable reference to occupied entry in TrackMap");
        self.base.into_mut()
    }

    pub fn insert(&mut self, value: V) -> V {
        debug!("Inserting value into occupied entry in TrackMap: value: {value:?}");
        self.base.insert(value)
    }

    pub fn get_mut(&mut self) -> &mut V {
        debug!("Getting mutable reference to occupied entry in TrackMap");
        self.base.get_mut()
    }

    pub fn get(&self) -> &V {
        debug!("Getting reference to occupied entry in TrackMap");
        self.base.get()
    }

    pub fn remove(self) -> (K, V) {
        self.base.remove_entry()
    }
}

impl<'a, K: std::hash::Hash + Eq + Debug, V: Debug> VacantTrackEntry<'a, K, V> {
    pub fn insert(self, value: V) -> &'a mut V {
        debug!("Inserting value into vacant entry in TrackMap: value: {value:?}");
        self.base.insert(value)
    }

    pub fn insert_entry(self, value: V) -> OccupiedTrackEntry<'a, K, V> {
        debug!(
            "Inserting value into vacant entry in TrackMap and returning occupied entry: value: {value:?}"
        );
        let base = self.base.insert_entry(value);
        OccupiedTrackEntry { base }
    }
}
