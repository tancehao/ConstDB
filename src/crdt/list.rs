use std::collections::LinkedList;
use crate::Bytes;

pub type List = Sequence<u128, Bytes>;

pub struct Sequence<T, V> {
    datas: LinkedList<(T, V)>, // T is an unique identifier, and V is it's value
}

impl<T, V> Sequence<T, V>
    where T: Ord
{
    pub fn insert_at(&mut self, value: (T, V), (after, before): (&T, &T)) {
        if self.datas.is_empty() {
            self.datas.push_back(value);
            return;
        }
        if &(self.datas.back().unwrap().0) <= after {
            self.datas.push_back(value);
            return;
        }
        if &(self.datas.front().unwrap().0) >= before {
            self.datas.push_front(value);
            return;
        }
        let mut pos = 0;
        let first = &(self.datas.front().unwrap().0);
        let mut prev: &T;
        let mut current = first;
        for i in self.datas.iter().map(|(i, _)| i) {
            prev = current;
            current = i;
            if prev <= after && before >= current {
                break;
            }
            pos += 1;
        }
        let mut prev = self.datas.split_off(pos);
        prev.push_back(value);
        std::mem::swap(&mut prev, &mut self.datas);
        self.datas.extend(prev);
    }
}