use std::{cmp::min, ops::Deref};

pub struct BoundedList<T> {
    items: Vec<T>,
    max_size: usize,
}

#[allow(unused)]
impl<T> BoundedList<T> {
    pub fn new(max_size: usize) -> Self {
        BoundedList {
            items: Vec::with_capacity(0),
            max_size,
        }
    }

    pub fn with_capacity(max_size: usize, capacity: usize) -> Self {
        BoundedList {
            items: Vec::with_capacity(min(max_size, capacity)),
            max_size,
        }
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    pub fn push(&mut self, item: T) -> Result<(), T> {
        if self.items.len() < self.max_size {
            self.items.push(item);
            Ok(())
        } else {
            Err(item)
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.items.len() == self.max_size
    }
}

impl<T> Deref for BoundedList<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.items
    }
}

impl<T> IntoIterator for BoundedList<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}
