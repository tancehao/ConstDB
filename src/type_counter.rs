use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::io::Write;

use bitflags::_core::cmp::max;

use crate::CstError;
use crate::cmd::NextArg;
use crate::link::Client;
use crate::object::{Encoding, Object};
use crate::resp::Message;
use crate::server::Server;
use crate::snapshot::{SnapshotLoader, SnapshotWriter};
use tokio::io::AsyncRead;

type VClock<T> = HashMap<u64, T>;

#[derive(Debug, Default, Clone)]
pub struct Counter {
    sum: i64,
    data: HashMap<u64, (i64, u64)>, // nodeid -> (value, modify uuid)
}

impl Counter {
    pub fn default() -> Self {
        Counter{
            sum: 0,
            data: HashMap::new(),
        }
    }

    #[inline]
    pub fn get(&self) -> i64 {
        self.sum
    }

    pub fn change(&mut self, actor: u64, value: i64, uuid: u64) -> i64 {
        match self.data.get_mut(&actor) {
            None => {
                self.data.insert(actor, (value, uuid));
                self.sum += value;
            }
            Some((v, t)) => {
                if *t < uuid {
                    *v += value;
                    self.sum += value;
                }
            }
        }
        self.sum
    }

    pub fn iter(&self) -> CounterIter {
        CounterIter{
            i: self.data.iter(),
        }
    }

    pub fn merge(&mut self, other: Counter) {
        for (nodeid, (v, t)) in self.data.iter_mut() {
            match other.data.get(nodeid) {
                Some((vv, tt)) => {
                    if *tt > *t {
                        *v = *vv;
                    } else if *tt == *t {
                        *v = max(*v, *vv);
                    }
                },
                None => {}
            }
        }
        for (nodeid, (vv, tt)) in other.data.iter() {
            match self.data.get_mut(nodeid) {
                Some((v, t)) => {
                    if *tt > *t {
                        *v = *vv;
                    } else if *tt == *t {
                        *v = max(*v, *vv);
                    }
                },
                None => {
                    self.data.insert(*nodeid, (*vv, *tt));
                },
            }
        }
        self.cal_sum();
    }

    fn cal_sum(&mut self) {
        self.sum = self.data.iter().map(|(_, (v, _))| *v).sum();
    }
}


impl Counter {
    pub fn describe(&self) -> Message {
        let data: Vec<Message> = self.data.iter().map(|(k, (v, t))| Message::Array(vec![Message::Integer(*k as i64), Message::Integer(*v), Message::Integer(*t as i64)])).collect();
        Message::Array(data)
    }

    pub fn save_snapshot<W: Write>(&self, dst: &mut SnapshotWriter<W>) -> Result<(), CstError> {
        dst.write_integer(self.data.len() as i64)?;
        for (nodeid, (v, t)) in self.data.iter() {
            dst.write_integer(*nodeid as i64)?;
            dst.write_integer(*v)?;
            dst.write_integer(*t as i64)?;
        }
        Ok(())
    }

    pub async fn load_snapshot<T: AsyncRead + Unpin>(src: &mut SnapshotLoader<T>) -> Result<Self, CstError> {
        let cnt = src.read_integer().await? as usize;
        let mut data = VClock::with_capacity(cnt);
        let mut total = 0;
        for _ in 0..cnt {
            let n = src.read_integer().await? as u64;
            let v = src.read_integer().await?;
            let t = src.read_integer().await? as u64;
            data.insert(n, (v, t));
            total += v;
        }
        Ok(Self{
            sum: total,
            data: data,
        })
    }
}

pub struct CounterIter<'a> {
    i: Iter<'a, u64, (i64, u64)>,
}

impl<'a> Iterator for CounterIter<'a> {
    type Item = (u64, (i64, u64));

    fn next(&mut self) -> Option<Self::Item> {
        self.i.next().map(|(nodeid, (value, time))| (*nodeid, (*value, *time)))
    }
}


pub fn delcnt_command(server: &mut Server, _client: Option<&mut Client>, _nodeid: u64, uuid: u64, args: Vec<Message>) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    let key_name = args.next_bytes()?;
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(Counter::default()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    match &mut o.enc {
        Encoding::Counter(c) => {
            // let cnt = args.next_i64()?;
            // *i += cnt;
            o.update_time = max(o.update_time, uuid);
            o.delete_time = max(o.delete_time, uuid);
            while let Ok(nodeid) = args.next_u64() {
                let v = args.next_i64()?;
                c.change(nodeid, v, uuid);
            }
            Ok(Message::None)
        }
        _ => return Err(CstError::InvalidType),
    }
}

pub fn incr_command(server: &mut Server, _client: Option<&mut Client>, nodeid: u64, uuid: u64, args: Vec<Message>) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    let key_name = args.next_bytes()?;
    //let o = server.db.query_or_insert(key_name, uuid)
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(Counter::default()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    //let mut db = HashMap::new();
    //let o = server.db.entry(key_name).or_insert(Object::new(Encoding::from(Counter::default()), uuid, 0).into());
    let c = o.enc.as_mut_counter()?;
    let v = c.change(nodeid, 1, uuid);
    o.updated_at(uuid);
    Ok(Message::Integer(v))
}

pub fn decr_command(server: &mut Server, _client: Option<&mut Client>, nodeid: u64, uuid: u64, args: Vec<Message>) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    let key_name = args.next_bytes()?;
    //let o = server.db.entry(key_name).or_insert(Object::new(Encoding::from(Counter::default()), uuid, 0).into());
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(Counter::default()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    let c = o.enc.as_mut_counter()?;
    let v = c.change(nodeid, -1, uuid);
    o.updated_at(uuid);
    Ok(Message::Integer(v))
}