use std::cmp::max;

use crate::{Bytes, CstError};
use crate::cmd::NextArg;
use crate::client::Client;
use crate::object::{Encoding, Object};
use crate::resp::Message;
use crate::server::Server;
use crate::crdt::lwwhash::Dict;

pub fn hset_command(server: &mut Server, _client: Option<&mut Client>, _nodeid: u64, uuid: u64, args: Vec<Message>) -> Result<Message, CstError> {
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

pub fn hdel_command(server: &mut Server, _client: Option<&mut Client>, _nodeid: u64, uuid: u64, args: Vec<Message>) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let fields = {
        let mut fields = vec![];
        while let Ok(field) = args.next_bytes() {
            fields.push(field);
        }
        fields
    };
    //let o = server.db.entry(key_name).or_insert(Object::new(Encoding::from(Dict::empty()), uuid, 0));
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

pub fn hget_command(server: &mut Server, _client: Option<&mut Client>, _nodeid: u64, uuid: u64, args: Vec<Message>) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let field_name = args.next_bytes()?;
    let res = match server.db.query(&key_name, uuid) {
        None => Message::Nil,
        Some(o) => {
            let s = o.enc.as_dict()?;
            s.get(&field_name).map(|x| Message::BulkString(x.clone())).unwrap_or(Message::Nil)
        }
    };
    Ok(res)
}

pub fn hgetall_command(server: &mut Server, _client: Option<&mut Client>, _nodeid: u64, uuid: u64, args: Vec<Message>) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let res = match server.db.query(&key_name, uuid) {
        None => Message::Nil,
        Some(o) => {
            let s = o.enc.as_dict()?;
            let kvs: Vec<Message> = s.iter().map(|(k, (_, v))| Message::Array(vec![Message::BulkString(k.clone()), Message::BulkString(v.clone())])).collect();
            Message::Array(kvs)
        }
    };
    Ok(res)
}

// deldict command can only be sent by our replicas
pub fn deldict_command(server: &mut Server, _client: Option<&mut Client>, _nodeid: u64, uuid: u64, args: Vec<Message>) -> Result<Message, CstError> {
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