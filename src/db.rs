use std::collections::{HashMap, LinkedList};
use std::io::Write;

use crate::object::{Encoding, Object};
use crate::snapshot::{
    SnapshotWriter, SNAPSHOT_FLAG_DATAS, SNAPSHOT_FLAG_DELETES, SNAPSHOT_FLAG_EXPIRES,
};
use crate::{Bytes, CstError};
use log::*;

const DB_INITIAL_SIZE: usize = 8096;

pub struct DB {
    data: HashMap<Bytes, Object>,
    expires: HashMap<Bytes, u64>, // key -> timestamp
    deletes: HashMap<Bytes, u64>,
    garbages: LinkedList<(Bytes, Option<Bytes>, u64)>, // (key, field/member, uuid)
}

impl DB {
    pub fn empty() -> Self {
        Self {
            data: HashMap::with_capacity(DB_INITIAL_SIZE),
            expires: HashMap::new(),
            deletes: HashMap::new(),
            garbages: LinkedList::default(),
        }
    }

    pub fn add(&mut self, key: Bytes, value: Object) {
        self.data.insert(key, value);
    }

    pub fn merge_entry(&mut self, key: Bytes, value: Object) {
        match self.data.get_mut(&key) {
            None => {
                self.data.insert(key, value);
            }
            Some(o) => {
                let en = value.enc.name();
                if o.merge(value).is_err() {
                    error!("Failed to merge key {} because there is a type conflict!, my type={}, other type={}", key.to_string(), o.enc.name(), en);
                }
            }
        }
    }

    // TODO
    pub fn contains_key(&mut self, _key: &Bytes) -> bool {
        false
    }

    // query the key at time t. If it's not contained in data, None is returned.
    // While if it's in data, but also in expires, and its expire time is smaller than t,
    // We insert it into deletes and also return it to the caller.
    pub fn query(&mut self, key: &Bytes, t: u64) -> Option<&mut Object> {
        let o = match self.data.get_mut(key) {
            None => return None,
            Some(o) => o,
        };
        if let Some(expire_time) = self.expires.get(key) {
            if o.alive() && o.created_before(*expire_time) && *expire_time <= t {
                o.delete_time = *expire_time;
                o.updated_at(*expire_time);
                let _ = self.deletes.insert(key.clone(), *expire_time);
            }
        }
        Some(o)
    }

    pub fn expire_at(&mut self, key: &Bytes, t: u64) {
        let ttl = self.expires.entry(key.clone()).or_default();
        *ttl = t;
    }

    pub fn delete(&mut self, key: &Bytes, t: u64) {
        let _ = self.deletes.insert(key.clone(), t);
        self.garbages.push_back((key.clone(), None, t));
    }

    pub fn delete_field(&mut self, key: &Bytes, field: &Bytes, t: u64) {
        self.garbages
            .push_back((key.clone(), Some(field.clone()), t));
    }

    pub fn gc(&mut self, tombstone: u64) {
        while let Some((key, field, t)) = self.garbages.pop_back() {
            if t > tombstone {
                break;
            }
            match field {
                None => match self.deletes.get(&key) {
                    None => {}
                    Some(v) => {
                        if *v == t {
                            self.deletes.remove(&key);
                        }
                    }
                },
                Some(f) => {
                    if let Some(v) = self.data.get_mut(&key) {
                        match &mut v.enc {
                            Encoding::LWWDict(dict) => {
                                if let Some(rt) = dict.remove_time(&f) {
                                    if rt < t {
                                        dict.remove_actually(&f);
                                    }
                                }
                            }
                            Encoding::LWWSet(set) => {
                                if let Some(rt) = set.remove_time(&f) {
                                    if rt < t {
                                        set.remove_actually(&f);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    // FIXME
    pub fn dump<W: Write>(&self, w: &mut SnapshotWriter<W>) -> Result<(), CstError> {
        let _ = w
            .write_byte(SNAPSHOT_FLAG_DATAS)?
            .write_integer(self.data.len() as i64)?;
        for (k, v) in self.data.iter() {
            w.write_entry(k.as_bytes(), v)?;
        }
        let _ = w
            .write_byte(SNAPSHOT_FLAG_EXPIRES)?
            .write_integer(self.expires.len() as i64)?;
        for (k, v) in self.expires.iter() {
            w.write_integer(k.len() as i64)?
                .write_bytes(k.as_bytes())?
                .write_integer(*v as i64)?;
        }
        let _ = w
            .write_byte(SNAPSHOT_FLAG_DELETES)?
            .write_integer(self.deletes.len() as i64)?;
        for (k, v) in self.deletes.iter() {
            w.write_integer(k.len() as i64)?
                .write_bytes(k.as_bytes())?
                .write_integer(*v as i64)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::db::DB;
    use crate::object::{Encoding, Object};
    use crate::Bytes;

    #[test]
    fn test_db() {
        let mut db = DB::empty();
        let (t1, t2, t3, t4, t5) = (1, 2, 3, 4, 5);
        let (k, v) = (Bytes::from("k1"), Bytes::from("v1"));
        db.add(k.clone(), Object::new(Encoding::Bytes(v.clone()), t2, 0));
        db.expire_at(&k, t2);
        assert!(db.query(&k, t1).is_some());
        assert!(db.query(&k, t2).is_some());
        //assert!(db.query(&k, t3).is_none());
    }
}
