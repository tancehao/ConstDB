use crate::resp::Message;
use crate::snapshot::{SnapshotLoader, SnapshotWriter};
use crate::{Bytes, CstError};
use std::cmp::max;
use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::Write;
use tokio::io::AsyncRead;

#[derive(Debug, Clone)]
pub struct LWWHash<K, V> {
    pub size: i32,
    pub add: HashMap<K, (u64, V)>, // key => (add_time, value)
    pub del: HashMap<K, u64>,      // key => del_time
}

impl<K, V> LWWHash<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn empty() -> Self {
        Self {
            size: 0,
            add: HashMap::new(),
            del: HashMap::new(),
        }
    }

    // Note: we don't care about the time when the `get` was called.
    // the timestamps inside in this structure is only used for solving conflicts.
    pub fn get(&self, k: &K) -> Option<&V> {
        match self.add.get(k) {
            None => None,
            Some((t1, v)) => match self.del.get(&k) {
                None => Some(v),
                Some(t2) => {
                    if *t1 >= *t2 {
                        Some(v)
                    } else {
                        None
                    }
                }
            },
        }
    }

    pub fn removed(&self, k: &K) -> bool {
        match (self.add.get(k), self.del.get(k)) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some((at, _)), Some(dt)) => *at < *dt,
        }
    }

    pub fn remove_time(&self, k: &K) -> Option<u64> {
        match (self.add.get(k), self.del.get(k)) {
            (_, None) => None,
            (None, Some(t)) => Some(*t),
            (Some((at, _)), Some(dt)) => {
                if *at < *dt {
                    Some(*dt)
                } else {
                    None
                }
            }
        }
    }

    pub fn remove_actually(&mut self, k: &K) {
        self.add.remove(k);
        self.del.remove(k);
    }

    pub fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        match self.add.get_mut(k) {
            None => None,
            Some((t1, v)) => match self.del.get(k) {
                None => Some(v),
                Some(t2) => {
                    if *t1 >= *t2 {
                        Some(v)
                    } else {
                        None
                    }
                }
            },
        }
    }

    pub fn set(&mut self, k: K, v: V, t: u64) -> bool {
        if let Some(v) = self.del.get(&k) {
            if *v > t {
                return false;
            }
        }
        match self.add.get_mut(&k) {
            Some((tt, vv)) => {
                if *tt > t {
                    return false;
                } else {
                    *vv = v;
                    *tt = t;
                }
            }
            None => {
                let _ = self.del.remove(&k);
                self.add.insert(k, (t, v));
            }
        }
        self.size += 1;
        true
    }

    pub fn rem(&mut self, k: &K, t: u64) -> bool {
        if let Some((v, _)) = self.add.get(k) {
            if *v > t {
                return false;
            }
        }
        match self.del.get_mut(k) {
            Some(tt) => {
                if *tt > t {
                    return false;
                } else {
                    *tt = t;
                }
            }
            None => {
                self.del.insert(k.clone(), t);
                let _ = self.add.remove(k);
            }
        }
        self.size -= 1;
        true
    }
}

pub type Dict = LWWHash<Bytes, Bytes>;

impl Dict {
    pub fn iter(&self) -> DictIter {
        DictIter {
            i: self.add.iter(),
            h: self,
        }
    }

    pub fn iter_all(&self) -> DictAllIter {
        DictAllIter {
            a: self.add.iter(),
            d: self.del.iter(),
        }
    }

    pub fn set_field(&mut self, field: Bytes, value: Bytes, uuid: u64) -> bool {
        self.set(field, value, uuid)
    }

    pub fn set_fields(&mut self, kvs: Vec<(Bytes, Bytes)>, uuid: u64) -> u32 {
        let mut cnt = 0;
        for (k, v) in kvs.into_iter() {
            if self.set_field(k, v, uuid) {
                cnt += 1;
            }
        }
        cnt
    }

    pub fn del_field(&mut self, field: &Bytes, uuid: u64) -> bool {
        self.rem(field, uuid)
    }

    pub fn del_fields(&mut self, fields: &[Bytes], uuid: u64) -> u32 {
        let mut s = 0;
        for field in fields {
            if self.del_field(field, uuid) {
                s += 1;
            }
        }
        s
    }

    pub fn merge(&mut self, other: Self) {
        for (k, (t, v)) in other.iter() {
            self.set(k.clone(), v.clone(), t);
        }
        unimplemented!()
    }

    pub fn describe(&self) -> Message {
        let a: Vec<Message> = self
            .add
            .iter()
            .map(|(k, (v, vv))| {
                Message::Array(vec![
                    Message::BulkString(k.clone()),
                    Message::Integer(*v as i64),
                    Message::BulkString(vv.clone()),
                ])
            })
            .collect();
        let d: Vec<Message> = self
            .del
            .iter()
            .map(|(k, v)| {
                Message::Array(vec![
                    Message::BulkString(k.clone()),
                    Message::Integer(*v as i64),
                ])
            })
            .collect();
        Message::Array(vec![Message::Array(a), Message::Array(d)])
    }

    pub fn save_snapshot<W: Write>(&self, dst: &mut SnapshotWriter<W>) -> Result<(), CstError> {
        dst.write_integer(self.add.len() as i64)?;
        for (k, (t, v)) in self.add.iter() {
            dst.write_integer(k.len() as i64)?;
            dst.write_bytes(k.as_bytes())?;
            dst.write_integer(*t as i64)?;
            dst.write_integer(v.len() as i64)?;
            dst.write_bytes(v.as_bytes())?;
        }
        dst.write_integer(self.del.len() as i64)?;
        for (k, t) in self.del.iter() {
            dst.write_integer(k.len() as i64)?;
            dst.write_bytes(k.as_bytes())?;
            dst.write_integer(*t as i64)?;
        }
        Ok(())
    }

    pub async fn load_snapshot<T: AsyncRead + Unpin>(
        src: &mut SnapshotLoader<T>,
    ) -> Result<Self, CstError> {
        let mut s = Self::empty();
        let add_cnt = src.read_integer().await? as usize;
        for _ in 0..add_cnt {
            let kl = src.read_integer().await? as usize;
            let k: Bytes = src.read_bytes(kl).await?.into();
            let t = src.read_integer().await? as u64;
            let vl = src.read_integer().await? as usize;
            let v: Bytes = src.read_bytes(vl).await?.into();
            let _ = s.set(k, v, t);
        }
        let del_cnt = src.read_integer().await? as usize;
        for _ in 0..del_cnt {
            let bl = src.read_integer().await? as usize;
            let k: Bytes = src.read_bytes(bl).await?.into();
            let t = src.read_integer().await? as u64;
            let _ = s.rem(&k, t);
        }
        Ok(s)
    }
}

pub struct DictIter<'a> {
    i: Iter<'a, Bytes, (u64, Bytes)>,
    h: &'a Dict,
}

impl<'a> Iterator for DictIter<'a> {
    type Item = (&'a Bytes, (u64, &'a Bytes));

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((k, (t, v))) = self.i.next() {
            if let Some(tt) = self.h.del.get(k) {
                if *tt > *t {
                    continue;
                }
            }
            return Some((k, (*t, v)));
        }
        None
    }
}

pub struct DictAllIter<'a> {
    a: Iter<'a, Bytes, (u64, Bytes)>,
    d: Iter<'a, Bytes, u64>,
}

impl<'a> Iterator for DictAllIter<'a> {
    type Item = (&'a Bytes, u64, bool);

    fn next(&mut self) -> Option<Self::Item> {
        self.a
            .next()
            .map(|(b, (u, _))| (b, *u, true))
            .or(self.d.next().map(|(b, u)| (b, *u, false)))
    }
}

pub type Set = LWWHash<Bytes, ()>;

impl Set {
    pub fn add_member(&mut self, member: Bytes, uuid: u64) -> bool {
        self.set(member, (), uuid)
    }

    pub fn add_members(&mut self, members: &[Bytes], uuid: u64) -> u64 {
        let mut s = 0;
        for member in members {
            if self.add_member(member.clone(), uuid) {
                s += 1;
            }
        }
        s
    }

    #[inline]
    pub fn remove_member(&mut self, member: &Bytes, uuid: u64) -> bool {
        self.rem(member, uuid)
    }

    pub fn remove_members(&mut self, members: &[Bytes], uuid: u64) -> u64 {
        let mut s = 0;
        for member in members {
            if self.remove_member(member, uuid) {
                s += 1;
            }
        }
        s
    }

    pub fn iter(&self) -> SetIter {
        SetIter {
            i: self.add.iter(),
            s: self,
        }
    }

    pub fn iter_all(&self) -> SetAllIter {
        SetAllIter {
            a: self.add.iter(),
            d: self.del.iter(),
        }
    }

    pub fn size(&self) -> u32 {
        max(0, self.size) as u32
    }

    pub fn describe(&self) -> Message {
        let a: Vec<Message> = self
            .add
            .iter()
            .map(|(k, (v, _))| {
                Message::Array(vec![
                    Message::BulkString(k.clone()),
                    Message::Integer(*v as i64),
                ])
            })
            .collect();
        let d: Vec<Message> = self
            .del
            .iter()
            .map(|(k, v)| {
                Message::Array(vec![
                    Message::BulkString(k.clone()),
                    Message::Integer(*v as i64),
                ])
            })
            .collect();
        Message::Array(vec![Message::Array(a), Message::Array(d)])
    }

    pub fn merge(&mut self, other: Self) {
        for (k, v) in other.iter() {
            self.set(k.clone(), (), v);
        }
    }

    pub fn save_snapshot<W: Write>(&self, dst: &mut SnapshotWriter<W>) -> Result<(), CstError> {
        dst.write_integer(self.add.len() as i64)?;
        for (k, (t, _)) in self.add.iter() {
            dst.write_integer(k.len() as i64)?;
            dst.write_bytes(k.as_bytes())?;
            dst.write_integer(*t as i64)?;
        }
        dst.write_integer(self.del.len() as i64)?;
        for (k, t) in self.del.iter() {
            dst.write_integer(k.len() as i64)?;
            dst.write_bytes(k.as_bytes())?;
            dst.write_integer(*t as i64)?;
        }
        Ok(())
    }

    pub async fn load_snapshot<T: AsyncRead + Unpin>(
        src: &mut SnapshotLoader<T>,
    ) -> Result<Self, CstError> {
        let mut s = Self::empty();
        let add_cnt = src.read_integer().await? as usize;
        for _ in 0..add_cnt {
            let bl = src.read_integer().await? as usize;
            let k: Bytes = src.read_bytes(bl).await?.into();
            let t = src.read_integer().await? as u64;
            let _ = s.add_member(k, t);
        }
        let del_cnt = src.read_integer().await? as usize;
        for _ in 0..del_cnt {
            let bl = src.read_integer().await? as usize;
            let k: Bytes = src.read_bytes(bl).await?.into();
            let t = src.read_integer().await? as u64;
            let _ = s.remove_member(&k, t);
        }
        Ok(s)
    }
}

pub struct SetIter<'a> {
    i: Iter<'a, Bytes, (u64, ())>,
    s: &'a Set,
}

impl<'a> Iterator for SetIter<'a> {
    type Item = (&'a Bytes, u64);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((k, (t, _))) = self.i.next() {
            if let Some(tt) = self.s.del.get(k) {
                if *tt > *t {
                    continue;
                }
            }
            return Some((k, *t));
        }
        None
    }
}

pub struct SetAllIter<'a> {
    a: Iter<'a, Bytes, (u64, ())>,
    d: Iter<'a, Bytes, u64>,
}

impl<'a> Iterator for SetAllIter<'a> {
    type Item = (&'a Bytes, u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.a
            .next()
            .map(|(b, (u, _))| (b, *u))
            .or(self.d.next().map(|(b, u)| (b, *u)))
    }
}
