use std::cmp::max;
use std::io::Write;

use crate::crdt::list::List;
use crate::crdt::lwwhash::{Dict, Set};
use crate::crdt::vclock::{Counter, MultiVersionVal};
use crate::resp::Message;
use crate::snapshot::{SnapshotLoader, SnapshotWriter};
use crate::{Bytes, CstError};
use tokio::io::AsyncRead;

#[derive(Debug, Clone)]
pub struct Object {
    pub create_time: u64,
    pub update_time: u64,
    pub delete_time: u64,
    pub enc: Encoding,
}

const OBJECT_ENC_COUNTER: u8 = 0;
const OBJECT_ENC_BYTES: u8 = 1;
const OBJECT_ENC_DICT: u8 = 2;
const OBJECT_ENC_SET: u8 = 3;
const OBJECT_ENC_LIST: u8 = 4;
const OBJECT_ENC_MVREG: u8 = 5;

impl Object {
    pub fn new(enc: Encoding, ct: u64, dt: u64) -> Self {
        Object {
            create_time: ct,
            update_time: 0,
            delete_time: dt,
            enc,
        }
    }

    #[inline]
    pub fn updated_at(&mut self, uuid: u64) {
        self.update_time = max(self.update_time, uuid);
    }

    pub fn delete_at(&mut self, uuid: u64) -> bool {
        let alive_before = self.alive();
        self.delete_time = max(self.delete_time, uuid);
        let alive_after = self.alive();
        alive_before && !alive_after
    }

    #[inline]
    pub fn alive(&self) -> bool {
        self.update_time >= self.delete_time
    }

    #[inline]
    pub fn created_before(&self, t: u64) -> bool {
        self.create_time < t
    }

    // apply the data in another object into the current one.
    // if an object was once of an encoding, and be deleted(softly) later, it still has that encoding.
    // that says, we avoid type conflicts even the user believes an older entry has been deleted.
    pub fn merge(&mut self, other: Object) -> Result<(), ()> {
        let (my_ct, my_dt) = (self.create_time, self.delete_time);
        let (his_ct, his_dt) = (other.create_time, other.delete_time);
        let (my_ut, his_ut) = (self.update_time, other.update_time);
        match (&mut self.enc, other.enc) {
            (Encoding::Counter(c), Encoding::Counter(oc)) => c.merge(*oc),
            (Encoding::Bytes(b), Encoding::Bytes(ob)) => {
                // TODO
                if my_ct < his_ct {
                    b.0 = ob.0.clone();
                }
                self.create_time = max(my_ct, his_ct);
                self.delete_time = max(my_dt, his_dt);
                self.update_time = max(my_ut, his_ut);
            }
            (Encoding::LWWDict(d), Encoding::LWWDict(od)) => d.merge(*od),
            (Encoding::LWWSet(s), Encoding::LWWSet(os)) => s.merge(*os),
            (Encoding::MVREG(s), Encoding::MVREG(os)) => s.merge(*os),
            _ => return Err(()),
        }
        Ok(())
    }

    pub fn save_snapshot<W: Write>(&self, w: &mut SnapshotWriter<W>) -> Result<(), CstError> {
        w.write_integer(self.create_time as i64)?;
        w.write_integer(self.update_time as i64)?;
        w.write_integer(self.delete_time as i64)?;
        match &self.enc {
            Encoding::Counter(i) => {
                w.write_byte(OBJECT_ENC_COUNTER)?;
                i.save_snapshot(w)
            }
            Encoding::Bytes(b) => {
                w.write_byte(OBJECT_ENC_BYTES)?;
                let _ = w.write_bytes(b.as_bytes())?;
                Ok(())
            }
            Encoding::LWWSet(s) => {
                w.write_byte(OBJECT_ENC_SET)?;
                s.save_snapshot(w)
            }
            Encoding::LWWDict(d) => {
                w.write_byte(OBJECT_ENC_DICT)?;
                d.save_snapshot(w)
            }
            Encoding::List(l) => {
                w.write_byte(OBJECT_ENC_LIST)?;
                l.save_snapshot(w)
            }
            Encoding::MVREG(r) => {
                w.write_byte(OBJECT_ENC_MVREG)?;
                r.save_snapshot(w)
            }
        }
    }

    pub async fn load_snapshot<T: AsyncRead + Unpin>(
        r: &mut SnapshotLoader<T>,
    ) -> Result<Self, CstError> {
        let (ct, mt, dt) = (
            r.read_integer().await? as u64,
            r.read_integer().await? as u64,
            r.read_integer().await? as u64,
        );
        let enc = match r.read_byte().await? {
            OBJECT_ENC_COUNTER => Encoding::from(Counter::load_snapshot(r).await?),
            OBJECT_ENC_BYTES => {
                let s = r.read_integer().await?;
                let d = r.read_bytes(s as usize).await?;
                Encoding::from(Bytes::from(d))
            }
            OBJECT_ENC_SET => Encoding::from(Set::load_snapshot(r).await?),
            OBJECT_ENC_DICT => Encoding::from(Dict::load_snapshot(r).await?),
            OBJECT_ENC_LIST => Encoding::from(List::load_snapshot(r).await?),
            OBJECT_ENC_MVREG => Encoding::from(MultiVersionVal::load_snapshot(r).await?),
            _ => return Err(CstError::InvalidType),
        };
        Ok(Object {
            create_time: ct,
            update_time: mt,
            delete_time: dt,
            enc,
        })
    }

    pub fn describe(&self) -> Message {
        let (t, m) = match &self.enc {
            Encoding::Counter(g) => ("counter", g.describe()),
            Encoding::Bytes(s) => ("bytes", Message::String(s.clone())),
            Encoding::LWWSet(t) => ("lwwset", t.describe()),
            Encoding::LWWDict(t) => ("lwwdict", t.describe()),
            Encoding::List(l) => ("list", l.describe()),
            Encoding::MVREG(l) => ("mvreg", l.describe()),
        };
        Message::Array(vec![
            Message::BulkString(format!("ct: {}", self.create_time).into()),
            Message::BulkString(format!("mt: {}", self.update_time).into()),
            Message::BulkString(format!("dt: {}", self.delete_time).into()),
            Message::BulkString(t.into()),
            m,
        ])
    }
}

#[derive(Debug, Clone)]
pub enum Encoding {
    Counter(Box<Counter>),
    Bytes(Bytes),
    LWWSet(Box<Set>),
    LWWDict(Box<Dict>),
    List(Box<List>),
    MVREG(Box<MultiVersionVal>),
}

impl Encoding {
    pub fn name(&self) -> &'static str {
        match self {
            Encoding::Counter(_) => "Counter",
            Encoding::Bytes(_) => "Bytes",
            Encoding::LWWDict(_) => "LWWDict",
            Encoding::LWWSet(_) => "LWWSet",
            Encoding::List(_) => "List",
            Encoding::MVREG(_) => "MVRegister",
        }
    }

    pub fn as_counter(&self) -> Result<&Counter, CstError> {
        match self {
            Encoding::Counter(c) => Ok(c),
            _ => Err(CstError::InvalidType),
        }
    }

    pub fn as_mut_counter(&mut self) -> Result<&mut Counter, CstError> {
        match self {
            Encoding::Counter(c) => Ok(c),
            _ => Err(CstError::InvalidType),
        }
    }

    pub fn as_set(&self) -> Result<&Set, CstError> {
        match self {
            Encoding::LWWSet(c) => Ok(c),
            _ => Err(CstError::InvalidType),
        }
    }

    pub fn as_mut_set(&mut self) -> Result<&mut Set, CstError> {
        match self {
            Encoding::LWWSet(c) => Ok(c),
            _ => Err(CstError::InvalidType),
        }
    }

    pub fn as_dict(&self) -> Result<&Dict, CstError> {
        match self {
            Encoding::LWWDict(c) => Ok(c),
            _ => Err(CstError::InvalidType),
        }
    }

    pub fn as_mut_dict(&mut self) -> Result<&mut Dict, CstError> {
        match self {
            Encoding::LWWDict(c) => Ok(c),
            _ => Err(CstError::InvalidType),
        }
    }

    pub fn as_list(&self) -> Result<&List, CstError> {
        match self {
            Encoding::List(c) => Ok(c),
            _ => Err(CstError::InvalidType),
        }
    }

    pub fn as_mut_list(&mut self) -> Result<&mut List, CstError> {
        match self {
            Encoding::List(c) => Ok(c),
            _ => Err(CstError::InvalidType),
        }
    }

    pub fn as_mvreg(&self) -> Result<&MultiVersionVal, CstError> {
        match self {
            Encoding::MVREG(c) => Ok(c),
            _ => Err(CstError::InvalidType),
        }
    }

    pub fn as_mut_mvreg(&mut self) -> Result<&mut MultiVersionVal, CstError> {
        match self {
            Encoding::MVREG(c) => Ok(c),
            _ => Err(CstError::InvalidType),
        }
    }
}

impl From<Counter> for Encoding {
    fn from(c: Counter) -> Self {
        Encoding::Counter(Box::new(c))
    }
}

impl From<Bytes> for Encoding {
    fn from(b: Bytes) -> Self {
        Encoding::Bytes(b)
    }
}

impl From<Set> for Encoding {
    fn from(c: Set) -> Self {
        Encoding::LWWSet(Box::new(c))
    }
}

impl From<Dict> for Encoding {
    fn from(c: Dict) -> Self {
        Encoding::LWWDict(Box::new(c))
    }
}

impl From<List> for Encoding {
    fn from(l: List) -> Self {
        Encoding::List(Box::new(l))
    }
}

impl From<MultiVersionVal> for Encoding {
    fn from(v: MultiVersionVal) -> Self {
        Encoding::MVREG(Box::new(v))
    }
}
