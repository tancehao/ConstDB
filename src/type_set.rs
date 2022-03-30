use std::cmp::max;
use std::option::Option::Some;
use tokio::macros::support::thread_rng_n;

use crate::cmd::NextArg;
use crate::crdt::lwwhash::Set;
use crate::link::Client;
use crate::object::{Encoding, Object};
use crate::resp::Message;
use crate::server::Server;
use crate::{Bytes, CstError};

pub fn sadd_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    let key_name = args.next_bytes()?;
    let members = {
        let mut members = vec![];
        while let Ok(member) = args.next_bytes() {
            members.push(member);
        }
        members
    };

    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(Set::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    let s = o.enc.as_mut_set()?;
    let mut cnt = s.add_members(members.as_slice(), uuid);

    // current replica add these members, and another replica delete the whole set later.
    if uuid < o.delete_time {
        s.remove_members(members.as_slice(), o.delete_time);
        cnt = 0;
    }
    o.updated_at(uuid);
    Ok(Message::Integer(cnt as i64))
}

pub fn srem_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    let key_name = args.next_bytes()?;
    let members = {
        let mut members = vec![];
        while let Ok(member) = args.next_bytes() {
            members.push(member);
        }
        members
    };
    //let o = server.db.entry(key_name).or_insert(Object::new(Encoding::from(Set::empty()), uuid, 0));
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(Set::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    let s = o.enc.as_mut_set()?;
    let cnt = s.remove_members(&members, uuid);
    o.updated_at(uuid);
    Ok(Message::Integer(cnt as i64))
}

pub fn smembers_command(
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
            let s = o.enc.as_set()?;
            let members: Vec<Message> = s
                .iter()
                .map(|(d, _)| Message::BulkString(d.clone()))
                .collect();
            Message::Array(members)
        }
    };
    Ok(res)
}

// TODO
pub fn spop_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    let key_name = args.next_bytes()?;

    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(Set::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    let s = o.enc.as_mut_set()?;
    let mut c = thread_rng_n(s.size());
    let mut m: Option<Bytes> = None;
    for (k, _) in s.iter() {
        c -= 1;
        if c == 0 {
            m = Some(k.clone());
            break;
        }
    }
    match m {
        Some(member) => {
            s.remove_member(&member, uuid);
            o.updated_at(uuid);
            Ok(Message::BulkString(member))
        }
        None => Ok(Message::Nil),
    }
}

// delset command can only be sent by our replicas
pub fn delset_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    let key_name = args.next_bytes()?;
    //let o = server.db.entry(key_name).or_insert(Object::new(Encoding::from(Set::empty()), uuid, 0).into());
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(Set::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    let s = o.enc.as_mut_set()?;
    let members: Vec<Bytes> = s.iter_all().map(|(x, _)| x.clone()).collect();
    let _ = s.remove_members(members.as_slice(), uuid);
    o.delete_time = max(o.delete_time, uuid);
    o.update_time = max(o.update_time, uuid);
    Ok(Message::None)
}
