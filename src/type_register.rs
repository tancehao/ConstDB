use crate::client::Client;
use crate::cmd::NextArg;
use crate::crdt::vclock::MultiVersionVal;
use crate::object::{Encoding, Object};
use crate::resp::{new_msg_ok, Message};
use crate::server::Server;
use crate::CstError;
use std::cmp::max;

pub fn mvget_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;

    match server.db.query(&key_name, uuid) {
        Some(o) => {
            if !o.alive() {
                return Ok(Message::Nil);
            }
            if o.create_time < o.delete_time {
                return Ok(Message::Nil);
            }
            let v = o.enc.as_mvreg()?;
            Ok(Message::Array(
                v.get_values()
                    .into_iter()
                    .map(|(node_id, value)| {
                        Message::Array(vec![
                            Message::Integer(node_id as i64),
                            Message::BulkString(value),
                        ])
                    })
                    .collect(),
            ))
        }
        None => Ok(Message::Nil),
    }
}

pub fn mvset_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let value = args.next_bytes()?;
    let hard = if let Ok(h) = args.next_string().map(|x| x.to_ascii_uppercase()) {
        match h.as_str() {
            "-HARD" => true,
            other => return Err(CstError::InvalidCmdOption("mvset", other.to_string())),
        }
    } else {
        false
    };
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(MultiVersionVal::default()), uuid, 0);
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    if o.update_time > uuid {
        return Ok(Message::Integer(0));
    }
    let v = o.enc.as_mut_mvreg()?;
    if hard {
        for (node_id, _) in v.get_values() {
            if v.del(node_id, uuid) {
                server.repl_backlog.replicate_cmd(
                    uuid,
                    "delmvreg",
                    vec![
                        Message::BulkString(key_name.as_bytes().into()),
                        Message::Integer(nodeid as i64),
                    ],
                );
            }
        }
    }
    v.set(nodeid, value, uuid);
    o.updated_at(uuid);
    Ok(new_msg_ok())
}

pub fn delmvreg_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let node_id = args.next_u64()?;
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(MultiVersionVal::default()), uuid, 0);
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };

    let v = o.enc.as_mut_mvreg()?;
    v.del(node_id, uuid);
    o.update_time = max(o.update_time, uuid);
    Ok(Message::None)
}
