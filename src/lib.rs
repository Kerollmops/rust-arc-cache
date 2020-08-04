use xlru_cache::{LruCache, IntoIter, Iter};
use std::borrow::Borrow;
use std::hash::Hash;
use std::iter;

pub struct ArcCache<K, V> {
    recent_set: LruCache<K, V>,
    recent_evicted: LruCache<K, ()>,
    frequent_set: LruCache<K, V>,
    frequent_evicted: LruCache<K, ()>,
    capacity: usize,
    p: usize,
    inserted: u64,
    evicted: u64,
    removed: u64,
}

impl<K, V> ArcCache<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new(capacity: usize) -> ArcCache<K, V> {
        assert_ne!(capacity, 0, "Cache length cannot be zero");
        ArcCache {
            recent_set: LruCache::new(capacity),
            recent_evicted: LruCache::new(capacity),
            frequent_set: LruCache::new(capacity),
            frequent_evicted: LruCache::new(capacity),
            capacity,
            p: 0,
            inserted: 0,
            evicted: 0,
            removed: 0,
        }
    }

    pub fn contains_key<Q: ?Sized>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.frequent_set.contains_key(key) || self.recent_set.contains_key(key)
    }

    pub fn insert<F>(&mut self, key: K, value: V, mut merge: F) -> (bool, Vec<(K, V)>)
    where F: FnMut(&mut V, V),
    {
        if let Some(old) = self.frequent_set.get_mut(&key) {
            merge(old, value);
            return (true, vec![]);
        }

        if let Some(mut old) = self.recent_set.remove(&key) {
            merge(&mut old, value);
            let (_, lru) = self.frequent_set.insert(key, old);
            return (true, lru.into_iter().collect());
        }

        let recent_evicted_len = self.recent_evicted.len();
        let frequent_evicted_len = self.frequent_evicted.len();
        let recent_len = self.recent_set.len();
        let frequent_len = self.frequent_set.len();

        if self.frequent_evicted.remove(&key).is_some() {
            let delta = if recent_evicted_len > frequent_evicted_len {
                recent_evicted_len / frequent_evicted_len
            } else {
                1
            };
            if delta < self.p {
                self.p -= delta;
            } else {
                self.p = 0;
            }
            let mut lrus = Vec::with_capacity(2);
            if recent_len + frequent_len >= self.capacity {
                if let Some(lru) = self.replace(true) {
                    lrus.push(lru);
                }
            }

            // TODO use the entry API instead
            let value = match self.frequent_set.remove(&key) {
                Some(mut old) => { merge(&mut old, value); old },
                None => value,
            };

            if let (_, Some(lru)) = self.frequent_set.insert(key, value) {
                lrus.push(lru);
            }
            return (true, lrus);
        }

        if self.recent_evicted.remove(&key).is_some() {
            let delta = if frequent_evicted_len > recent_evicted_len {
                frequent_evicted_len / recent_evicted_len
            } else {
                1
            };
            if delta <= self.capacity - self.p {
                self.p += delta;
            } else {
                self.p = self.capacity;
            }
            let mut lrus = Vec::with_capacity(2);
            if recent_len + frequent_len >= self.capacity {
                if let Some(lru) = self.replace(false) {
                    lrus.push(lru);
                }
            }

            // TODO use the entry API instead
            let value = match self.frequent_set.remove(&key) {
                Some(mut old) => { merge(&mut old, value); old },
                None => value,
            };

            if let (_, Some(lru)) = self.frequent_set.insert(key, value) {
                lrus.push(lru);
            }
            return (true, lrus);
        }

        let mut lrus = Vec::with_capacity(2);

        if recent_len + frequent_len >= self.capacity {
            if let Some(lru) = self.replace(false) {
                lrus.push(lru);
            }
        }

        if recent_evicted_len > self.capacity - self.p {
            self.recent_evicted.remove_lru();
            self.evicted += 1;
        }

        if frequent_evicted_len > self.p {
            self.frequent_evicted.remove_lru();
            self.evicted += 1;
        }

        // TODO use the entry API instead
        let value = match self.recent_set.remove(&key) {
            Some(mut old) => { merge(&mut old, value); old },
            None => value,
        };

        if let (_, Some(lru)) = self.recent_set.insert(key, value) {
            lrus.push(lru);
        }
        self.inserted += 1;

        (false, lrus)
    }

    pub fn peek_mut(&mut self, key: &K) -> Option<&mut V>
    where
        K: Clone + Hash + Eq,
    {
        if let Some(entry) = self.frequent_set.peek_mut(key) {
            Some(entry)
        } else {
            self.recent_set.peek_mut(key)
        }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V>
    where
        K: Clone + Hash + Eq,
    {
        if let Some(value) = self.recent_set.remove(&key) {
            self.frequent_set.insert((*key).clone(), value);
        }
        self.frequent_set.get_mut(key)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let removed_frequent = self.frequent_set.remove(&key);
        let removed_recent = self.recent_set.remove(&key);

        self.frequent_evicted.remove(&key);
        self.recent_evicted.remove(&key);

        match removed_frequent.or(removed_recent) {
            Some(value) => {
                self.removed += 1;
                Some(value)
            },
            None => None
        }
    }

    fn replace(&mut self, frequent_evicted_contains_key: bool) -> Option<(K, V)> {
        let recent_set_len = self.recent_set.len();
        if recent_set_len > 0
            && (recent_set_len > self.p
                || (recent_set_len == self.p && frequent_evicted_contains_key))
        {
            if let Some((old_key, old_value)) = self.recent_set.remove_lru() {
                self.recent_evicted.insert(old_key.clone(), ());
                return Some((old_key, old_value));
            }
        } else {
            if let Some((old_key, old_value)) = self.frequent_set.remove_lru() {
                self.frequent_evicted.insert(old_key.clone(), ());
                return Some((old_key, old_value));
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.recent_set.len() + self.frequent_set.len()
    }

    pub fn frequent_len(&self) -> usize {
        self.frequent_set.len()
    }

    pub fn recent_len(&self) -> usize {
        self.recent_set.len()
    }

    pub fn inserted(&self) -> u64 {
        self.inserted
    }

    pub fn evicted(&self) -> u64 {
        self.evicted
    }

    pub fn removed(&self) -> u64 {
        self.removed
    }

    pub fn iter(&self) -> iter::Chain<Iter<K, V>, Iter<K, V>> {
        self.into_iter()
    }
}

impl<K: Eq + Hash, V> IntoIterator for ArcCache<K, V> {
    type Item = (K, V);
    type IntoIter = iter::Chain<IntoIter<K, V>, IntoIter<K, V>>;

    fn into_iter(self) -> Self::IntoIter {
        let recent = self.recent_set.into_iter();
        let frequent = self.frequent_set.into_iter();
        recent.chain(frequent)
    }
}

impl<'a, K: Eq + Hash, V> IntoIterator for &'a ArcCache<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = iter::Chain<Iter<'a, K, V>, Iter<'a, K, V>>;

    fn into_iter(self) -> Self::IntoIter {
        let recent = self.recent_set.iter();
        let frequent = self.frequent_set.iter();
        recent.chain(frequent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replace_and_drop<T>(old: &mut T, new: T) {
        let _ = std::mem::replace(old, new);
    }

    #[test]
    fn easy() {
        let mut arc = ArcCache::new(2);

        arc.insert("testkey", "testvalue", replace_and_drop);
        assert!(arc.contains_key(&"testkey"));

        arc.insert("testkey2", "testvalue2", replace_and_drop);
        assert!(arc.contains_key(&"testkey2"));

        arc.insert("testkey3", "testvalue3", replace_and_drop);
        assert!(arc.contains_key(&"testkey3"));
        assert!(arc.contains_key(&"testkey2"));
        assert!(!arc.contains_key(&"testkey"));

        arc.insert("testkey", "testvalue", replace_and_drop);
        assert!(arc.get_mut(&"testkey").is_some());
        assert!(arc.get_mut(&"testkey-nx").is_none());
    }

    #[test]
    fn poped_out() {
        let mut arc = ArcCache::new(2);

        let (_, _poped) = arc.insert("1", "a", replace_and_drop);
        let (_, poped) = arc.insert("2", "b", replace_and_drop);
        assert!(poped.is_empty());

        let (_, poped) = arc.insert("3", "c", replace_and_drop);
        assert_eq!(poped, vec![("1", "a")]);

        let (_, poped) = arc.insert("2", "b", replace_and_drop);
        assert!(poped.is_empty());

        let (_, poped) = arc.insert("1", "a", replace_and_drop);
        assert_eq!(poped, vec![("2", "b")]);
    }
}
