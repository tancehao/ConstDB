use std::cmp::max;

use crate::client::Client;
use crate::cmd::NextArg;
use crate::crdt::lwwhash::Dict;
use crate::object::{Encoding, Object};
use crate::resp::Message;
use crate::server::Server;
use crate::{Bytes, CstError};
use bitflags::_core::cmp::min;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};

pub fn hset_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let kvs = {
        let mut kvs = vec![];
        while let Ok(field) = args.next_bytes() {
            kvs.push((field, args.next_bytes()?));
        }
        kvs
    };
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(Dict::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };

    let mut cnt = 0;
    let d = o.enc.as_mut_dict()?;
    for (k, v) in kvs.iter() {
        if d.set_field(k.clone(), v.clone(), uuid) {
            cnt += 1;
        }
    }

    if uuid < o.delete_time {
        for (k, _) in kvs.iter() {
            d.del_field(k, o.delete_time);
            cnt = 0;
        }
    }
    o.updated_at(uuid);
    Ok(Message::Integer(cnt as i64))
}

pub fn hdel_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let fields = {
        let mut fields = vec![];
        while let Ok(field) = args.next_bytes() {
            fields.push(field);
        }
        fields
    };
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(Dict::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    let s = o.enc.as_mut_dict()?;
    let cnt = s.del_fields(fields.as_slice(), uuid);
    o.updated_at(uuid);
    Ok(Message::Integer(cnt as i64))
}

pub fn hget_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let field_name = args.next_bytes()?;
    let res = match server.db.query(&key_name, uuid) {
        None => Message::Nil,
        Some(o) => {
            let s = o.enc.as_dict()?;
            s.get(&field_name)
                .map(|x| Message::BulkString(x.clone()))
                .unwrap_or(Message::Nil)
        }
    };
    Ok(res)
}

pub fn hgetall_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let res = match server.db.query(&key_name, uuid) {
        None => Message::Nil,
        Some(o) => {
            let s = o.enc.as_dict()?;
            let kvs: Vec<Message> = s
                .iter()
                .map(|(k, (_, v))| {
                    Message::Array(vec![
                        Message::BulkString(k.clone()),
                        Message::BulkString(v.clone()),
                    ])
                })
                .collect();
            Message::Array(kvs)
        }
    };
    Ok(res)
}

// deldict command can only be sent by our replicas
pub fn deldict_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    //let o = server.db.entry(key_name).or_insert(Object::new(Encoding::from(Dict::empty()), uuid, 0).into());
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(Dict::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    let d = o.enc.as_mut_dict()?;
    let members: Vec<Bytes> = d.iter_all().map(|(x, _, _)| x.clone()).collect();
    let _ = d.del_fields(members.as_slice(), uuid);
    o.delete_time = max(o.delete_time, uuid);
    o.update_time = max(o.update_time, uuid);
    Ok(Message::None)
}

pub fn hkeys_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    let key_name = args.next_bytes()?;
    let res = match server.db.query(&key_name, uuid) {
        None => Message::Nil,
        Some(o) => {
            let s = o.enc.as_dict()?;
            Message::Array(
                s.iter()
                    .map(|(k, _)| Message::BulkString(k.clone()))
                    .collect(),
            )
        }
    };
    Ok(res)
}

pub fn hlen_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    let key_name = args.next_bytes()?;
    let res = match server.db.query(&key_name, uuid) {
        None => Message::Nil,
        Some(o) => {
            let s = o.enc.as_dict()?;
            Message::Integer(s.iter().count() as i64)
        }
    };
    Ok(res)
}

pub fn hmget_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let mut fields = vec![args.next_bytes()?];
    while let Ok(field) = args.next_bytes() {
        fields.push(field);
    }
    let res = match server.db.query(&key_name, uuid) {
        None => fields.into_iter().map(|_| Message::Nil).collect(),
        Some(o) => {
            let s = o.enc.as_dict()?;
            fields
                .into_iter()
                .map(|f| {
                    s.get(&f)
                        .map_or(Message::Nil, |x| Message::BulkString(x.clone()))
                })
                .collect()
        }
    };
    Ok(Message::Array(res))
}

pub fn hrandfield_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let count = args.next_i64().ok();
    let with_values = if let Ok(wv) = args.next_string().map(|x| x.to_ascii_uppercase()) {
        match wv.as_str() {
            "WITHVALUES" => true,
            opt => return Err(CstError::InvalidCmdOption("HRANDFIELD", opt.to_string())),
        }
    } else {
        false
    };

    let res = match server.db.query(&key_name, uuid) {
        None => Message::Nil,
        Some(o) => {
            let s = o.enc.as_dict()?;
            if let Some(cnt) = &count {
                let mut kvs: Vec<(&Bytes, &Bytes)> = s.iter().map(|(k, (_, v))| (k, v)).collect();
                kvs.shuffle(&mut thread_rng());
                let real_cnt = if *cnt < 0 {
                    -*cnt as i32
                } else {
                    min(*cnt as i32, s.size)
                };
                if real_cnt > s.size {
                    let v = {
                        let n = thread_rng().gen::<i32>() % s.size;
                        kvs[n as usize]
                    };
                    kvs.push(v);
                }
                let mut res = vec![];
                for (k, v) in kvs {
                    res.push(Message::BulkString(k.clone()));
                    if with_values {
                        res.push(Message::BulkString(v.clone()));
                    }
                }
                Message::Array(res)
            } else {
                let n = thread_rng().gen::<usize>() % (s.size as usize);
                let field = s.iter().skip(n).map(|(x, _)| x.clone()).next().unwrap();
                Message::BulkString(field)
            }
        }
    };
    Ok(res)
}

pub fn hvals_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let mut fields = vec![args.next_bytes()?];
    while let Ok(field) = args.next_bytes() {
        fields.push(field);
    }
    let res = match server.db.query(&key_name, uuid) {
        None => vec![],
        Some(o) => {
            let s = o.enc.as_dict()?;
            s.iter()
                .map(|(_, (_, v))| Message::BulkString(v.clone()))
                .collect()
        }
    };
    Ok(Message::Array(res))
}
