use crate::cmd::NextArg;
use crate::crdt::list::{List, Number, NumberWithUUIDNodeID};
use crate::client::Client;
use crate::object::{Encoding, Object};
use crate::repl_backlog::ReplBacklog;
use crate::resp::{new_msg_ok, Message};
use crate::server::Server;
use crate::{Bytes, CstError};
use std::cmp::{max, min};
use std::result::Result::Ok;

pub fn rpop_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key = args.next_bytes()?;
    match server.db.query(&key, uuid) {
        None => Ok(Message::Nil),
        Some(o) => {
            let l = o.enc.as_mut_list()?;
            let (cnt, cnt_specified) = match args.next_u64() {
                Ok(c) => (c, true),
                Err(_) => (1, false),
            };
            let mut eles = Vec::with_capacity(min(cnt as usize, 64));
            for _ in 0..cnt {
                match l.pop_back() {
                    None => break,
                    Some((n, v)) => {
                        eles.push(Message::BulkString(v));
                        replicate_list_modify(&mut server.repl_backlog, uuid, &key, &n, Op::Rem);
                    }
                }
            }
            if eles.is_empty() {
                Ok(Message::Nil)
            } else {
                if cnt_specified {
                    Ok(Message::Array(eles))
                } else {
                    Ok(eles.pop().unwrap())
                }
            }
        }
    }
}

pub fn lpop_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key = args.next_bytes()?;
    match server.db.query(&key, uuid) {
        None => Ok(Message::Nil),
        Some(o) => {
            let l = o.enc.as_mut_list()?;
            let (cnt, cnt_specified) = match args.next_u64() {
                Ok(c) => (c, true),
                Err(_) => (1, false),
            };
            let mut eles = Vec::with_capacity(min(cnt as usize, 64));
            for _ in 0..cnt {
                match l.pop_front() {
                    None => break,
                    Some((n, v)) => {
                        eles.push(Message::BulkString(v));
                        replicate_list_modify(&mut server.repl_backlog, uuid, &key, &n, Op::Rem);
                    }
                }
            }
            if eles.is_empty() {
                Ok(Message::Nil)
            } else {
                o.updated_at(uuid);
                if cnt_specified {
                    Ok(Message::Array(eles))
                } else {
                    Ok(eles.pop().unwrap())
                }
            }
        }
    }
}

pub fn lrem_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let cnt = args.next_i64()?;
    if cnt < 0 {
        return Err(CstError::InvalidRequestMsg(
            "Not supported to start from the back yet".to_string(),
        ));
    }
    let ele = args.next_bytes()?;
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(List::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    let l = o.enc.as_mut_list()?;
    let indices: Vec<NumberWithUUIDNodeID> = l
        .iter()
        .filter(|(_, x)| x == &ele)
        .map(|(x, _)| x.clone())
        .take(cnt as usize)
        .collect();

    // TODO need to optimize when the cursor features of LinkList is stable
    let mut cnt = 0;
    for index in indices {
        if l.remove(index.clone()).is_some() {
            cnt += 1;
        }
        replicate_list_modify(&mut server.repl_backlog, uuid, &key_name, &index, Op::Rem);
    }
    o.updated_at(uuid);
    Ok(Message::Integer(cnt))
}

pub fn lindex_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let mut pos = args.next_i64()?;
    let resp = match server.db.query(&key_name, uuid) {
        None => Message::Nil,
        Some(o) => {
            let l = o.enc.as_list()?;
            let size = l.len() as i64;
            pos = if pos < 0 { size + pos } else { pos };
            if pos > size - 1 {
                Message::Nil
            } else {
                let (_, ele) = l.iter().skip(pos as usize).next().unwrap();
                Message::BulkString(ele.clone())
            }
        }
    };
    Ok(resp)
}

pub fn llen_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let resp = match server.db.query(&key_name, uuid) {
        None => 0,
        Some(o) => {
            let l = o.enc.as_list()?;
            l.len() as i64
        }
    };
    Ok(Message::Integer(resp))
}

pub fn lpos_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let target = args.next_bytes()?;
    let (mut rank, mut count, mut maxlen) = (None, None, None);
    while let Ok(option) = args.next_string().map(|x| x.to_uppercase()) {
        match option.as_str() {
            "RANK" => rank = Some(args.next_i64()?),
            "COUNT" => count = Some(args.next_u64()? as usize),
            "MAXLEN" => maxlen = Some(args.next_u64()? as usize),
            other => {
                return Err(CstError::InvalidCmdOption("LPOP", other.to_string()));
            }
        }
    }
    let resp = match server.db.query(&key_name, uuid) {
        None => Message::Nil,
        Some(o) => {
            let l = o.enc.as_list()?;
            let size = l.len();
            let rank = rank.unwrap_or(0);
            // TODO Optimization needed. We should traverse from the back if the rank is less than 0.
            let maxlen = maxlen.unwrap_or(size);
            let mut pos = 0;
            let mut found = vec![];
            for (_, ele) in l.iter().take(maxlen) {
                if ele == &target {
                    found.push(pos);
                }
                pos += 1;
            }
            let count = count.unwrap_or(found.len());
            let rank = if rank < 0 {
                found.len() as i64 + rank
            } else {
                rank
            };
            if count > 1 {
                let pos = if rank < 0 || rank >= found.len() as i64 {
                    vec![]
                } else {
                    found
                        .iter()
                        .skip(rank as usize)
                        .take(count)
                        .map(|x| Message::Integer(*x))
                        .collect()
                };
                Message::Array(pos)
            } else {
                if rank < 0 || rank >= found.len() as i64 {
                    Message::Nil
                } else {
                    Message::Integer(found[0])
                }
            }
        }
    };
    Ok(resp)
}

pub fn lrange_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let (mut start, mut end) = (args.next_i64()?, args.next_i64()?);
    let o = match server.db.query(&key_name, uuid) {
        None => return Ok(new_msg_ok()),
        Some(o) => o,
    };
    let l = o.enc.as_list()?;
    let size = l.len() as i64;
    start = if start < 0 { size + start } else { start };
    start = max(start, 0);
    end = if end < 0 { size + end } else { end };
    end = min(end, size - 1);
    let eles: Vec<Message> = l
        .iter()
        .skip(start as usize)
        .take((end - start) as usize + 1)
        .map(|(_, ele)| Message::BulkString(ele.clone()))
        .collect();
    Ok(Message::Array(eles))
}

pub fn lset_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let (mut position, value) = (args.next_i64()?, args.next_bytes()?);
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(List::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    let l = o.enc.as_mut_list()?;
    let size = l.len() as i64;
    position = if position < 0 {
        size + position
    } else {
        position
    };
    if position >= size {
        return Ok(Message::Error("index out of range".into()));
    }
    let (p, ele) = l.iter_mut().skip(position as usize).next().unwrap();
    // Note: `set` is different from `insert`. we must use lww policy here.
    if p.uuid() < uuid {
        *p = NumberWithUUIDNodeID::new(p.number(), uuid, nodeid);
        *ele = value.clone();
        replicate_list_modify(&mut server.repl_backlog, uuid, &key_name, p, Op::Set(value));
    }

    Ok(new_msg_ok())
}

pub fn ltrim_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let (mut start, mut end) = (args.next_i64()?, args.next_i64()?);
    let o = match server.db.query(&key_name, uuid) {
        None => return Ok(new_msg_ok()),
        Some(o) => o,
    };
    let l = o.enc.as_mut_list()?;
    let size = l.len() as i64;
    start = if start < 0 { size + start } else { start };
    start = max(start, 0);
    end = if end < 0 { size + end } else { end };
    end = min(end, size - 1);
    let right = size - end;
    for _ in 0..start {
        match l.pop_front() {
            None => return Err(CstError::SystemError),
            Some((n, _)) => {
                replicate_list_modify(&mut server.repl_backlog, uuid, &key_name, &n, Op::Rem)
            }
        }
    }
    for _ in 0..right {
        match l.pop_back() {
            None => return Err(CstError::SystemError),
            Some((n, _)) => {
                replicate_list_modify(&mut server.repl_backlog, uuid, &key_name, &n, Op::Rem)
            }
        }
    }
    o.updated_at(uuid);
    Ok(new_msg_ok())
}

pub fn dellist_command(
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
            let o = Object::new(Encoding::from(List::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    o.delete_time = max(o.delete_time, uuid);
    o.update_time = max(o.update_time, uuid);
    let l = o.enc.as_mut_list()?;
    while let Ok(number_str) = args.next_string() {
        let number = match Number::unmarshal(&number_str) {
            Some(n) => n,
            None => {
                error!("Invalid request message. The string {} can not be unmarshaled into a Number type", number_str);
                continue;
            }
        };
        let n = NumberWithUUIDNodeID::new(number, args.next_u64()?, args.next_u64()?);
        let _ = l.remove(n);
    }
    Ok(Message::None)
}

pub fn linsert_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let is_before = match args.next_string()?.to_uppercase().as_str() {
        "BEFORE" => true,
        "AFTER" => false,
        other => {
            return Err(CstError::InvalidCmdOption("LINSERT", other.to_string()));
        }
    };
    let pivot = args.next_bytes()?;
    let ele = args.next_bytes()?;
    let o = match server.db.query(&key_name, uuid) {
        None => return Ok(Message::Nil),
        Some(o) => o,
    };

    let l = o.enc.as_mut_list()?;
    let mut current = match l.front() {
        None => return Ok(Message::Integer(-1)),
        Some((n, _)) => n.number().clone(),
    };
    let mut prev: Option<Number> = None;
    let mut found = false;
    for (n, v) in l.iter() {
        prev = Some(current.clone());
        current = n.number().clone();
        if found {
            break;
        }
        if v == &pivot {
            found = true;
            if is_before {
                break;
            }
        }
    }
    let resp = if !found {
        -1
    } else {
        let number = match prev {
            None => current.prev(),
            Some(n) => n.middle(Some(&current)).ok_or(CstError::SystemError)?
        };
        let n = NumberWithUUIDNodeID::new(number, uuid, nodeid);
        replicate_list_modify(
            &mut server.repl_backlog,
            uuid,
            &key_name,
            &n,
            Op::Add(ele.clone()),
        );
        l.insert_at(n, ele);
        let l = l.len() as i64;
        o.updated_at(uuid);
        l
    };
    Ok(Message::Integer(resp))
}

pub fn rpush_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let mut eles = vec![args.next_bytes()?];
    while let Ok(ele) = args.next_bytes() {
        eles.push(ele);
    }
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(List::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };

    let mut cnt = 0;
    let l = o.enc.as_mut_list()?;
    for ele in eles.iter() {
        let number = match l.back() {
            None => Number::empty(),
            Some((n, _)) => n.number().middle(None).unwrap(),
        };
        let n = NumberWithUUIDNodeID::new(number.clone(), uuid, nodeid);
        l.insert_at(n.clone(), ele.clone());
        replicate_list_modify(
            &mut server.repl_backlog,
            uuid,
            &key_name,
            &n,
            Op::Add(ele.clone().into()),
        );
        cnt += 1;
    }

    o.updated_at(uuid);
    Ok(Message::Integer(cnt as i64))
}

pub fn lpush_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let mut eles = vec![args.next_bytes()?];
    while let Ok(ele) = args.next_bytes() {
        eles.push(ele);
    }
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(List::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };

    let mut cnt = 0;
    let l = o.enc.as_mut_list()?;
    for ele in eles.iter() {
        let number = match l.front() {
            None => Number::empty(),
            Some((n, _)) => n.number().prev(),
        };
        let n = NumberWithUUIDNodeID::new(number.clone(), uuid, nodeid);
        l.insert_at(n.clone(), ele.clone());
        replicate_list_modify(
            &mut server.repl_backlog,
            uuid,
            &key_name,
            &n,
            Op::Add(ele.clone()),
        );
        cnt += 1;
    }

    o.updated_at(uuid);
    Ok(Message::Integer(cnt as i64))
}

enum Op {
    Rem,
    Add(Bytes),
    Set(Bytes),
}

fn replicate_list_modify(
    log: &mut ReplBacklog,
    uuid: u64,
    key_name: &Bytes,
    n: &NumberWithUUIDNodeID,
    op: Op,
) {
    let (sub_cmd, ele_arg) = match op {
        Op::Rem => ("rem", None),
        Op::Add(e) => ("add", Some(Message::BulkString(e))),
        Op::Set(e) => ("set", Some(Message::BulkString(e))),
    };
    let mut args = vec![
        Message::BulkString(sub_cmd.into()),
        Message::BulkString(key_name.clone().into()),
        Message::BulkString(n.number().marshal().into()),
        Message::Integer(n.uuid() as i64),
        Message::Integer(n.nodeid() as i64),
    ];
    if let Some(ele) = ele_arg {
        args.push(ele);
    }
    log.replicate_cmd(uuid, "lchangeat", args);
}

pub fn lchangeat_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let is_add = match args.next_string()?.to_ascii_lowercase().as_str() {
        "add" => true,
        "rem" => false,
        other => return Err(CstError::InvalidCmdOption("LCHANGEAT", other.to_string())),
    };
    let key_name = args.next_bytes()?;
    let number = match Number::unmarshal(&args.next_string()?) {
        Some(n) => n,
        None => {
            error!(
                "Received an invalid message which should be able to unserialized into a Number"
            );
            return Err(CstError::InvalidRequestMsg(
                "invalid serialized number".into(),
            ));
        }
    };
    let n = NumberWithUUIDNodeID::new(number, uuid, nodeid);
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(List::empty()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    let l = o.enc.as_mut_list()?;
    if is_add {
        l.insert_at(n, args.next_bytes()?);
    } else {
        let _ = l.remove(n);
    }
    o.updated_at(uuid);
    Ok(Message::Nil)
}
