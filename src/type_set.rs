use std::cmp::{max, min};
use std::option::Option::Some;
use tokio::macros::support::thread_rng_n;

use crate::cmd::NextArg;
use crate::crdt::lwwhash::Set;
use crate::client::Client;
use crate::object::{Encoding, Object};
use crate::resp::Message;
use crate::server::Server;
use crate::{Bytes, CstError};
use std::collections::{HashSet, HashMap};
use crate::resp::Message::Integer;
use rand::{thread_rng, Rng};
use rand::seq::SliceRandom;

pub fn sadd_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
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
    let mut args = args.into_iter().skip(1);
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
    let mut args = args.into_iter().skip(1);
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
    let mut args = args.into_iter().skip(1);
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
    let mut args = args.into_iter().skip(1);
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
    let members: Vec<Bytes> = s.iter_all().map(|(x, _)| x.clone()).collect();
    let _ = s.remove_members(members.as_slice(), uuid);
    o.delete_time = max(o.delete_time, uuid);
    o.update_time = max(o.update_time, uuid);
    Ok(Message::None)
}

pub fn scard_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let o = match server.db.query(&key_name, uuid) {
        None => 0,
        Some(o) => {
            let s = o.enc.as_set()?;
            s.size as i64
        },
    };
    Ok(Message::Integer(o))
}

pub fn sinter_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let mut keys = vec![args.next_bytes()?];
    while let Ok(key) = args.next_bytes() {
        keys.push(key);
    }
    let mut res: HashMap<Bytes, usize> = HashMap::new();
    scan_multi_set(server, &keys, uuid, |_, x| {
        let a = res.entry(x.clone()).or_insert(0);
        *a += 1;
    });
    let res = res.into_iter()
        .filter(|(_, x)| *x == keys.len())
        .map(|(x, _)| Message::BulkString(x)).collect();
    Ok(Message::Array(res))
}

pub fn sinterstore_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let dst_key = args.next_bytes()?;
    let mut keys = vec![args.next_bytes()?];
    while let Ok(key) = args.next_bytes() {
        keys.push(key);
    }

    let mut res: HashMap<Bytes, usize> = HashMap::new();
    scan_multi_set(server, &keys, uuid, |_, x| {
        let a = res.entry(x.clone()).or_insert(0);
        *a += 1;
    });
    let res: Vec<Bytes> = res.into_iter()
        .filter(|(_, x)| *x == keys.len())
        .map(|(x, _)| x).collect();

    let o = match server.db.query(&dst_key, uuid) {
        Some(v) => v,
        None => {
            let o = Object::new(Encoding::from(Set::empty()), uuid, 0).into();
            server.db.add(dst_key.clone(), o);
            server.db.query(&dst_key, uuid).unwrap()
        }
    };
    let s = o.enc.as_mut_set()?;
    for member in res {
        s.add_member(member, uuid);
    }
    let s = s.size as i64;
    o.updated_at(uuid);
    Ok(Message::Integer(s))
}

pub fn sintercard_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let mut keys = vec![args.next_bytes()?];
    while let Ok(key) = args.next_bytes() {
        keys.push(key);
    }
    let mut res: HashMap<Bytes, usize> = HashMap::new();
    scan_multi_set(server, &keys, uuid, |_, x| {
        let a = res.entry(x.clone()).or_insert(0);
        *a += 1;
    });
    let cnt = res.into_iter()
        .filter(|(_, x)| *x == keys.len())
        .map(|(x, _)| x).count();
    Ok(Message::Integer(cnt as i64))
}

fn scan_multi_set<F>(server: &mut Server, keys: &[Bytes], uuid: u64, mut f: F)
    where F: FnMut(usize, &Bytes) -> ()
{
    let mut i = 0;
    for k in keys {
        match server.db.query(&k, uuid) {
            None => {},
            Some(o) => {
                match o.enc.as_set() {
                    Err(_) => continue,
                    Ok(s) => {
                        for (k, _) in s.iter() {
                            f(i, k);
                        }
                    }
                }
            }
        }
        i += 1;
    }
}

pub fn sdiff_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let mut keys = vec![args.next_bytes()?];
    while let Ok(key) = args.next_bytes() {
        keys.push(key);
    }
    let mut res_first = HashSet::new();
    let mut res = HashSet::new();
    scan_multi_set(server, &keys, uuid, |i, x| {
        if i == 0 {
            res_first.insert(x.clone());
        } else {
            res.insert(x.clone());
        }
    });
    let members = res_first
        .difference(&res).into_iter()
        .map(|x| Message::BulkString(x.clone()))
        .collect();
    Ok(Message::Array(members))
}

pub fn sdiffstore_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let dst_key = args.next_bytes()?;
    let mut keys = vec![args.next_bytes()?];
    while let Ok(key) = args.next_bytes() {
        keys.push(key);
    }
    let mut res_first = HashSet::new();
    let mut res = HashSet::new();
    scan_multi_set(server, &keys, uuid, |i, x| {
        if i == 0 {
            res_first.insert(x.clone());
        } else {
            res.insert(x.clone());
        }
    });
    let o = match server.db.query(&dst_key, uuid) {
        Some(v) => v,
        None => {
            let o = Object::new(Encoding::from(Set::empty()), uuid, 0).into();
            server.db.add(dst_key.clone(), o);
            server.db.query(&dst_key, uuid).unwrap()
        }
    };
    let s = o.enc.as_mut_set()?;
    for member in res_first.difference(&res).into_iter() {
        s.add_member(member.clone(), uuid);
    }
    let s = s.size as i64;
    o.updated_at(uuid);
    Ok(Message::Integer(s))
}

pub fn sismember_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let member = args.next_bytes()?;
    let is = match server.db.query(&key_name, uuid) {
        None => 0,
        Some(o) => {
            let s = o.enc.as_set()?;
            s.get(&member).map(|_| 1).unwrap_or_default()
        }
    };
    Ok(Message::Integer(is))
}

pub fn smismember_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let mut members = vec![args.next_bytes()?];
    while let Ok(member) = args.next_bytes() {
        members.push(member);
    }
    let res = match server.db.query(&key_name, uuid) {
        None => vec![],
        Some(o) => {
            let s = o.enc.as_set()?;
            members.into_iter()
                .map(|x| s.get(&x).map(|_x| 1).unwrap_or_default())
                .map(|x| Message::Integer(x))
                .collect()
        }
    };
    Ok(Message::Array(res))
}

pub fn smove_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let src_key = args.next_bytes()?;
    let dst_key = args.next_bytes()?;
    let member = args.next_bytes()?;
    {
        match server.db.query(&dst_key, uuid) {
            None => return Ok(Integer(0)),
            Some(o) => {
                let _ = o.enc.as_set()?;
            },
        }
    }

    let src = match server.db.query(&src_key, uuid) {
        None => return Ok(Message::Integer(0)),
        Some(o) => o,
    };
    let src_set = src.enc.as_mut_set()?;
    if !src_set.remove_member(&member, uuid) {
        return Ok(Message::Integer(0));
    }
    src.updated_at(uuid);

    let dst = match server.db.query(&dst_key, uuid) {
        None => return Ok(Message::Integer(0)),
        Some(o) => o,
    };
    let dst_set = dst.enc.as_mut_set().unwrap();
    dst_set.add_member(member, uuid);
    dst.updated_at(uuid);
    Ok(Message::Integer(1))
}

pub fn srandmember_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let count = args.next_i64().ok();

    let res = match server.db.query(&key_name, uuid) {
        None => Message::Nil,
        Some(o) => {
            let s = o.enc.as_set()?;
            if let Some(cnt) = &count {
                let mut members: Vec<&Bytes> = s.iter().map(|(x, _)| x).collect();
                members.shuffle(&mut thread_rng());
                let real_cnt = if *cnt < 0 {
                    -*cnt as i32
                } else {
                    min(*cnt as i32, s.size)
                };
                if real_cnt > s.size {
                    let v = {
                        let n = thread_rng().gen::<i32>() % s.size;
                        members[n as usize]
                    };
                    members.push(v);
                }

                Message::Array(members.into_iter().map(|x| Message::BulkString(x.clone())).collect())
            } else {
                let n = thread_rng().gen::<usize>() % (s.size as usize);
                let field = s.iter().skip(n).map(|(x, _)| x.clone()).next().unwrap();
                Message::BulkString(field)
            }
        },
    };

    Ok(res)
}

pub fn sunion_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let mut keys = vec![args.next_bytes()?];
    while let Ok(key) = args.next_bytes() {
        keys.push(key);
    }
    let mut res = HashSet::new();
    scan_multi_set(server, &keys, uuid, |_, x| {
        res.insert(x.clone());
    });
    let res = res.into_iter()
        .map(|x| Message::BulkString(x)).collect();
    Ok(Message::Array(res))
}

pub fn sunionstore_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let dst_key = args.next_bytes()?;
    let mut keys = vec![args.next_bytes()?];
    while let Ok(key) = args.next_bytes() {
        keys.push(key);
    }

    let mut res = HashSet::new();
    scan_multi_set(server, &keys, uuid, |_, x| {
        res.insert(x.clone());
    });

    let o = match server.db.query(&dst_key, uuid) {
        Some(v) => v,
        None => {
            let o = Object::new(Encoding::from(Set::empty()), uuid, 0).into();
            server.db.add(dst_key.clone(), o);
            server.db.query(&dst_key, uuid).unwrap()
        }
    };
    let s = o.enc.as_mut_set()?;
    for member in res {
        s.add_member(member, uuid);
    }
    let s = s.size as i64;
    o.updated_at(uuid);
    Ok(Message::Integer(s))
}