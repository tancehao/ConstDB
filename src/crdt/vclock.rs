use crate::resp::Message;
use crate::snapshot::{SnapshotLoader, SnapshotWriter};
use crate::{Bytes, CstError};
use std::cmp::min;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Write;
use tokio::io::AsyncRead;

type VClock<T> = MiniMap<T>;

#[allow(unused)]
pub struct MiniMap<T> {
    values: Vec<(u64, (T, u64))>, // node_id -> (value, modify time)
}

impl<T: Debug> Debug for MiniMap<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.values.fmt(f)
    }
}

impl<T> Default for MiniMap<T> {
    fn default() -> Self {
        MiniMap { values: vec![] }
    }
}

impl<T: Clone> Clone for MiniMap<T> {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
        }
    }
}

impl<T: Clone> MiniMap<T> {
    pub fn iter(&self) -> std::slice::Iter<(u64, (T, u64))> {
        self.values.iter()
    }

    pub fn get(&self, k: &u64) -> Option<&(T, u64)> {
        match self.values.binary_search_by(|(key, _)| key.cmp(k)) {
            Ok(i) => self.values.get(i).map(|(_, v)| v),
            Err(_) => None,
        }
    }

    pub fn get_mut(&mut self, k: &u64) -> Option<&mut (T, u64)> {
        match self.values.binary_search_by(|(key, _)| key.cmp(k)) {
            Ok(i) => self.values.get_mut(i).map(|(_, x)| x),
            Err(_) => None,
        }
    }

    pub fn set(&mut self, k: u64, v: T, uuid: u64) {
        match self.values.binary_search_by(|(key, _)| key.cmp(&k)) {
            Ok(i) => self.values[i] = (k, (v, uuid)),
            Err(i) => self.values.insert(i, (k, (v, uuid))),
        }
    }

    pub fn merge(&mut self, other: Self) {
        let mut values = Vec::with_capacity(min(self.values.len(), other.values.len()));
        let mut s = self.values.iter();
        let mut o = other.values.iter();
        let (mut ss, mut oo) = (None, None);
        loop {
            ss = ss.or(s.next());
            oo = oo.or(o.next());
            let vv = match (&oo, &ss) {
                (None, None) => break,
                (None, Some((nodeid, (v, t)))) => {
                    ss = None;
                    (*nodeid, (v.clone(), *t))
                }
                (Some((nodeid, (v, t))), None) => {
                    oo = None;
                    (*nodeid, (v.clone(), *t))
                }
                (Some((on, (ov, ot))), Some((sn, (sv, st)))) => {
                    if *on < *sn {
                        oo = None;
                        (*on, (ov.clone(), *ot))
                    } else {
                        ss = None;
                        (*sn, (sv.clone(), *st))
                    }
                }
            };
            if values.len() == values.capacity() {
                values.reserve(1);
            }
            values.push(vv);
        }
        self.values = values;
    }
}

pub type Counter = VClock<i64>; // (value, modify uuid)

impl Counter {
    pub fn empty() -> Self {
        let mut s = Self::default();
        s.set(0, 0, 0);
        s
    }

    #[inline]
    pub fn get_total(&self) -> i64 {
        self.get(&0).unwrap().0
    }

    pub fn change(&mut self, actor: u64, value: i64, uuid: u64) -> i64 {
        match self.get_mut(&(actor + 1)) {
            Some(v) => {
                v.0 += value;
                v.1 = uuid;
            }
            None => self.set(actor + 1, value, uuid),
        }
        let sum = self.get_mut(&0).unwrap();
        sum.0 += value;
        sum.1 = uuid;
        sum.0
    }

    pub fn save_snapshot<W: Write>(&self, dst: &mut SnapshotWriter<W>) -> Result<(), CstError> {
        dst.write_integer(self.values.len() as i64)?;
        for (nodeid, v) in self.values.iter() {
            dst.write_integer(*nodeid as i64)?
                .write_integer(v.0)?
                .write_u64(v.1)?;
        }
        Ok(())
    }

    pub async fn load_snapshot<T: AsyncRead + Unpin>(
        loader: &mut SnapshotLoader<T>,
    ) -> Result<Self, CstError> {
        let size = loader.read_integer().await? as usize;
        let mut values = Vec::with_capacity(size);
        for _ in 0..size {
            let node_id = loader.read_integer().await? as u64;
            let cnt = loader.read_integer().await? as i64;
            let uuid = loader.read_u64().await? as u64;
            values.push((node_id, (cnt, uuid)));
        }
        Ok(Self { values })
    }

    pub fn describe(&self) -> Message {
        Message::Array(
            self.values
                .iter()
                .map(|(k, (c, t))| {
                    Message::Array(vec![
                        Message::Integer(*k as i64),
                        Message::Integer(*c),
                        Message::Integer(*t as i64),
                    ])
                })
                .collect(),
        )
    }
}

pub type MultiVersionVal = VClock<Bytes>;

impl MultiVersionVal {
    pub fn get_values(&self) -> Vec<(u64, Bytes)> {
        self.values
            .iter()
            .map(|(n, (d, _))| (*n, d.clone()))
            .collect()
    }

    pub fn hard_set(&mut self, node_id: u64, value: Bytes, uuid: u64) {
        self.values = vec![(node_id, (value, uuid))];
    }

    pub fn del(&mut self, node_id: u64, uuid: u64) -> bool {
        let len_before = self.values.len();
        let mut values = vec![];
        std::mem::swap(&mut self.values, &mut values);
        self.values = values
            .into_iter()
            .filter(|(x, v)| *x != node_id || v.1 >= uuid)
            .collect();
        let len_after = self.values.len();
        len_before < len_after
    }

    pub fn save_snapshot<W: Write>(&self, dst: &mut SnapshotWriter<W>) -> Result<(), CstError> {
        dst.write_integer(self.values.len() as i64)?;
        for (nodeid, v) in self.values.iter() {
            dst.write_integer(*nodeid as i64)?
                .write_integer(v.0.len() as i64)?
                .write_bytes(v.0.as_bytes())?
                .write_u64(v.1)?;
        }
        Ok(())
    }

    pub async fn load_snapshot<T: AsyncRead + Unpin>(
        loader: &mut SnapshotLoader<T>,
    ) -> Result<Self, CstError> {
        let size = loader.read_integer().await? as usize;
        let mut values = Vec::with_capacity(size);
        for _ in 0..size {
            let node_id = loader.read_integer().await? as u64;
            let data_len = loader.read_integer().await? as usize;
            let data = Bytes::from(loader.read_bytes(data_len).await?);
            let uuid = loader.read_u64().await? as u64;
            values.push((node_id, (data, uuid)));
        }
        Ok(Self { values })
    }

    pub fn describe(&self) -> Message {
        Message::Array(
            self.values
                .iter()
                .map(|(k, (d, t))| {
                    Message::Array(vec![
                        Message::Integer(*k as i64),
                        Message::BulkString(d.clone()),
                        Message::Integer(*t as i64),
                    ])
                })
                .collect(),
        )
    }
}
