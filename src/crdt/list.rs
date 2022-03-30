use crate::resp::Message;
use crate::snapshot::{SnapshotLoader, SnapshotWriter};
use crate::{Bytes, CstError};
use serde::Serializer;
use std::cmp::Ordering;
use std::collections::LinkedList;
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::vec::Vec;
use tokio::io::AsyncRead;

// we need to define a new number type to make it possible to find a number between any two numbers.
// eg: [1, {any number}] is between 1 and 2, and [1,2,{any number}] is between [1,2] and [1,3]
#[derive(Clone, Debug)]
pub struct Number(Vec<u8>);

impl Display for Number {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return f.serialize_str("()");
        }
        f.serialize_str("(")?;
        f.serialize_u8(self.0[0])?;
        for i in self.0.iter().skip(1) {
            f.write_fmt(format_args!(",{}", *i))?;
        }
        f.serialize_str(")")
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        let mut o = other.0.iter();
        for i in self.0.iter() {
            if i != o.next().unwrap() {
                return false;
            }
        }
        true
    }
}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut o = other.0.iter();
        for i in self.0.iter() {
            match o.next() {
                None => return Some(Ordering::Greater),
                Some(oo) => {
                    if *i < *oo {
                        return Some(Ordering::Less);
                    } else if *i > *oo {
                        return Some(Ordering::Greater);
                    }
                }
            }
        }
        if o.next().is_none() {
            Some(Ordering::Equal)
        } else {
            Some(Ordering::Less)
        }
    }
}

impl From<Vec<u8>> for Number {
    fn from(ints: Vec<u8>) -> Self {
        Self(ints)
    }
}

impl Number {
    pub fn empty() -> Self {
        Number(vec![0])
    }

    pub fn between(&self, range: (&Self, &Self)) -> bool {
        self > range.0 && self < range.1
    }

    fn next(&self) -> Self {
        let mut n = self.0.clone();
        let len = self.0.len();
        n[len - 1] = self.0[len - 1] + (u8::MAX - self.0[len - 1]) / 2;
        Number(n)
    }

    pub fn prev(&self) -> Self {
        let mut n = self.0.clone();
        let len = self.0.len();
        if n[len - 1] > 0 {
            n[len - 1] = self.0[len - 1] / 2;
        } else {
            n[len - 1] = u8::MAX / 2;
            for i in 1..len {
                let d = n.get_mut(len - i - 1).unwrap();
                if *d > 0 {
                    *d -= 1;
                    break;
                } else {
                    *d = u8::MAX;
                }
            }
        }
        Number(n)
    }

    // find a number that is greater than myself and less than `less_than`
    pub fn middle(&self, less_than: Option<&Self>) -> Option<Self> {
        let less_than = match less_than {
            Some(l) => l,
            None => return Some(self.next()),
        };
        let mut ints = Vec::with_capacity(self.0.len());
        let mut s = self.0.iter();
        let mut o = less_than.0.iter();
        loop {
            match (s.next(), o.next()) {
                (Some(ss), Some(oo)) => {
                    if *ss == *oo {
                        ints.push(*ss);
                    } else if *ss > *oo {
                        return None;
                    } else {
                        if *oo - *ss > 1 {
                            ints.push(*ss + (*oo - *ss) / 2);
                        } else {
                            ints.push(*ss);
                            let s_next = match s.next() {
                                None => 0,
                                Some(n) => *n,
                            };
                            ints.push(s_next + (u8::MAX - s_next) / 2);
                        }
                        break;
                    }
                }
                (None, Some(oo)) => {
                    ints.push(*oo / 2);
                    break;
                }
                (_, None) => return None, // myself is no less than `less_than`
            }
        }
        Some(Number(ints))
    }

    pub fn marshal(&self) -> String {
        format!("{}", self)
    }

    pub fn unmarshal(d: &String) -> Option<Self> {
        let len = d.len();
        if len < 2 || d.as_bytes()[0] != b'(' || d.as_bytes()[len - 1] != b')' {
            return None;
        }

        let mut numbers = vec![];
        for number in d[1..len - 1].split(',') {
            match number.parse::<u8>() {
                Err(_) => return None,
                Ok(u) => numbers.push(u),
            }
        }
        Some(Number(numbers))
    }

    pub fn save_snapshot<W: Write>(&self, dst: &mut SnapshotWriter<W>) -> Result<(), CstError> {
        dst.write_integer(self.0.len() as i64)?
            .write_bytes(self.0.as_slice())?;
        Ok(())
    }

    pub async fn load_snapshot<T: AsyncRead + Unpin>(
        src: &mut SnapshotLoader<T>,
    ) -> Result<Self, CstError> {
        let len = src.read_integer().await? as usize;
        let v = src.read_bytes(len).await?;
        Ok(Number(v))
    }
}

#[cfg(test)]
mod test {
    use crate::crdt::list::Number;

    #[test]
    fn test_number() {
        assert!(Number(vec![1]) < Number(vec![2]));
        assert!(Number(vec![1, 1]) < Number(vec![2]));
        assert!(Number(vec![1, 1]) < Number(vec![1, 2]));
        assert!(Number(vec![1]) < Number(vec![1, 2]));
        let (a, b, c) = (Number(vec![1]), Number(vec![1, 1]), Number(vec![2]));
        assert!(b.between((&a, &c)));
        assert!(b.middle(&a).is_none());
        let ab = a.middle(&b).unwrap();
        assert!(ab > a);
        assert!(ab < b);
        let bc = b.middle(&c).unwrap();
        assert!(bc < c);
        assert!(bc > b);
    }
}

#[derive(Debug, Clone)]
pub struct NumberWithUUIDNodeID(Number, u64, u64); // The first field `Number` determines which position an element is in a list.
                                                   // And if two elements have the same position, we then determine their actual
                                                   // position by the uuid which can be used as a timestamp. We place a node_id
                                                   // to the third field to make unsure its uniqueness.

impl Display for NumberWithUUIDNodeID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "(number={}, uuid={}, node_id={})",
            self.0, self.1, self.2
        ))
    }
}

impl PartialOrd for NumberWithUUIDNodeID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.0.partial_cmp(&other.0) {
            Some(Ordering::Equal) => match self.1.partial_cmp(&other.1) {
                Some(Ordering::Equal) => self.2.partial_cmp(&other.2),
                other => other,
            },
            other => other,
        }
    }
}

impl PartialEq for NumberWithUUIDNodeID {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1 && self.2 == other.2
    }
}

impl NumberWithUUIDNodeID {
    pub fn new(number: Number, uuid: u64, nodeid: u64) -> Self {
        NumberWithUUIDNodeID(number, uuid, nodeid)
    }

    #[inline]
    pub fn number(&self) -> Number {
        self.0.clone()
    }

    #[inline]
    pub fn uuid(&self) -> u64 {
        self.1
    }

    #[inline]
    pub fn nodeid(&self) -> u64 {
        self.2
    }
}

pub type List = Sequence<NumberWithUUIDNodeID, Bytes>;

impl List {
    pub fn empty() -> Self {
        Self {
            datas: LinkedList::new(),
        }
    }

    // pub fn set(&mut self, position: usize, uuid: u64, nodeid: u64, value: V) {
    //     for (p, ele) in self.iter_mut() {}
    // }

    // TODO this function is O(n^2), we should use O(n) method once the cursor features of LinkedList are stable.
    pub fn merge(&mut self, mut other: Self) {
        while let Some((t, v)) = other.pop_front() {
            self.insert_at(t, v);
        }
    }

    pub fn trim(&mut self, start: usize, end: usize) {
        if start >= end {
            return;
        }
        let mut back = self.datas.split_off(start);
        back.split_off(end - start);
        self.datas = back;
    }

    pub fn describe(&self) -> Message {
        Message::Array(
            self.datas
                .iter()
                .map(|x| {
                    Message::Array(vec![
                        Message::BulkString(format!("{}", x.0).into()),
                        Message::BulkString(x.1.clone()),
                    ])
                })
                .collect(),
        )
    }

    pub fn save_snapshot<W: Write>(&self, dst: &mut SnapshotWriter<W>) -> Result<(), CstError> {
        dst.write_integer(self.datas.len() as i64)?;
        for (p, v) in self.datas.iter() {
            p.0.save_snapshot(dst)?;
            dst.write_u64(p.1)?
                .write_u64(p.2)?
                .write_integer(v.len() as i64)?
                .write_bytes(v.as_bytes())?;
        }
        Ok(())
    }

    pub async fn load_snapshot<T: AsyncRead + Unpin>(
        src: &mut SnapshotLoader<T>,
    ) -> Result<Self, CstError> {
        let mut l = Self::empty();
        let len = src.read_integer().await?;
        for _ in 0..len {
            let number = Number::load_snapshot(src).await?;
            let uuid = src.read_u64().await?;
            let nodeid = src.read_u64().await?;
            let vlen = src.read_integer().await? as usize;
            let v: Bytes = src.read_bytes(vlen).await?.into();
            l.insert_at(NumberWithUUIDNodeID(number, uuid, nodeid), v);
        }
        Ok(l)
    }
}

// Some methods of std::collections::LinkedList are unstable.
// So we have to use some stupid solutions currently.
#[derive(Debug, Clone)]
pub struct Sequence<T, V> {
    datas: LinkedList<(T, V)>, // T is an unique index, and V is it's value
}

impl<T, V> Deref for Sequence<T, V> {
    type Target = LinkedList<(T, V)>;

    fn deref(&self) -> &Self::Target {
        &self.datas
    }
}

impl<T, V> DerefMut for Sequence<T, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.datas
    }
}

impl<T, V> Sequence<T, V>
where
    T: PartialOrd + Debug,
{
    pub fn get(&self, index: usize) -> Option<&(T, V)> {
        self.datas.iter().skip(index).next()
    }

    pub fn len(&self) -> usize {
        self.datas.len()
    }

    // TODO need to optimize once the cursor features of LinkedList are stable.
    pub fn insert_at(&mut self, position: T, value: V) {
        if self.datas.is_empty() {
            self.datas.push_back((position, value));
            return;
        }
        if &(self.datas.back().unwrap().0) <= &position {
            self.datas.push_back((position, value));
            return;
        }
        if &(self.datas.front().unwrap().0) >= &position {
            self.datas.push_front((position, value));
            return;
        }
        let mut pos = 0;
        for i in self.datas.iter().map(|(i, _)| i) {
            if &position < i {
                break;
            }
            pos += 1;
        }
        let back_half = self.datas.split_off(pos);
        self.datas.push_back((position, value));
        self.datas.extend(back_half);
    }

    pub fn remove(&mut self, position: T) -> Option<V> {
        let mut pos = -1 as i64;
        for (t, _) in self.datas.iter() {
            if &position == t {
                break;
            }
            pos += 1;
        }
        if pos > 0 {
            let back_half = self.datas.split_off((pos + 1) as usize);
            let r = self.datas.pop_back().map(|(_, x)| x);
            self.datas.extend(back_half);
            return r;
        }
        None
    }
}

#[cfg(test)]
mod test_list {
    use crate::crdt::list::{List, Number, NumberWithUUIDNodeID};
    use crate::Bytes;
    use std::collections::{LinkedList, VecDeque};
    use tokio::macros::support::thread_rng_n;

    #[test]
    fn test_list() {
        let mut l = List::empty();
        let mut uuid = 1;
        let mut values = vec![];
        for i in 0..50 {
            let n = Number(vec![i]);
            uuid += 1;
            let v: Bytes = format!("number{}", i).into();
            let nu = NumberWithUUIDNodeID(n, uuid, 0);
            l.insert_at(nu.clone(), v.clone());
            values.push((nu, v));
        }
        for i in 0..50 {
            let idx = thread_rng_n(49 + i);
            let node = l.get(idx as usize).unwrap();
            let next_node = l.get(idx as usize + 1).unwrap();
            let (node_number, next_node_number) = (node.0.number(), next_node.0.number());
            let middle_number = node_number.middle(&next_node_number).unwrap();
            let nu = NumberWithUUIDNodeID(middle_number, uuid, 0);
            let v: Bytes = format!("new_number{}", i).into();
            l.insert_at(nu.clone(), v.clone());
            values.push((nu, v));
            uuid += 1;
        }

        values.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        values.reverse();
        while let Some((t, v)) = l.pop_front() {
            let (tt, vv) = values.pop().unwrap();
            assert_eq!(t, tt);
            assert_eq!(v, vv);
        }
    }
}
