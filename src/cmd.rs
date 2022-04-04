use std::collections::HashMap;
use std::fmt::Display;

use std::cmp::max;
use std::fmt::{Debug, Formatter};

use crate::lib::utils::bytes2i64;
use crate::object::{Encoding, Object};
use crate::replica::{forget_command, meet_command, replicas_command, sync_command};
use crate::resp::get_int_bytes;
use crate::resp::{new_msg_ok, Message};
use crate::server::Server;
use crate::stats::info_command;
use crate::type_counter::{decr_command, delcnt_command, incr_command};
use crate::type_hash::{
    deldict_command, hdel_command, hget_command, hgetall_command, hset_command,
};
use crate::type_list::{
    dellist_command, lchangeat_command, lindex_command, linsert_command, llen_command,
    lpop_command, lpos_command, lpush_command, lrange_command, lrem_command, lset_command,
    ltrim_command, rpop_command, rpush_command,
};
use crate::type_register::{delmvreg_command, mvset_command, mvget_command};
use crate::type_set::{delset_command, sadd_command, smembers_command, spop_command, srem_command};
use crate::{Bytes, CstError, now_mil};
use crate::client::{Client, client_command, wait_command};

#[derive(Debug)]
pub struct Cmd {
    args: Vec<Message>,
    time_mil: u64,
    command: &'static Command,
}

impl Display for Cmd {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "cmd_name: `\x1b[1;32m{}\x1b[0m`, args: [",
            self.command.name
        ))?;
        for arg in self.args.iter() {
            f.write_fmt(format_args!("{}", arg))?;
        }
        f.write_str("]")
    }
}

impl Cmd {
    pub fn new(args: Vec<Message>) -> Result<Cmd, CstError> {
        let command = match args.first() {
            None => Err(CstError::WrongArity),
            Some(cmd) => match cmd {
                Message::BulkString(cmd) => {
                    COMMANDS.get(cmd.as_bytes().to_ascii_lowercase().as_slice())
                        .ok_or(CstError::UnknownCmd(cmd.clone().into()))
                }
                _ => Err(CstError::InvalidRequestMsg("the first argument should be of bulk string type".to_string()))
            }
        }?;

        Ok(Cmd{command, args, time_mil: now_mil()})
    }

    pub fn exec(
        &self,
        client: Option<&mut Client>,
        server: &mut Server,
    ) -> Result<Message, CstError> {
        server.metrics.incr_cmd_processed();
        if self.command.flags & COMMAND_REPL_ONLY > 0 {
            return Err(CstError::UnknownCmd(self.command.name.to_string()));
        }
        let (nodeid, uuid) = {
            let is_write = self.command.flags | COMMAND_WRITE > 0;
            (server.node_id, server.next_uuid(self.time_mil, is_write))
        };
        self.exec_detail(
            server,
            client,
            nodeid,
            uuid,
            (self.command.flags & COMMAND_WRITE) > 0
                && (self.command.flags & COMMAND_NO_REPLICATE == 0),
        )
    }

    // execute the command. when this function is call by replicate command, it does not duplicate again to other replicas.
    pub fn exec_detail(
        &self,
        server: &mut Server,
        client: Option<&mut Client>,
        nodeid: u64,
        uuid: u64,
        repl: bool,
    ) -> Result<Message, CstError> {
        let r = (self.command.handler)(server, client, nodeid, uuid, self.args.clone());
        debug!(
            "Executed command {}, nodeid={}, uuid={}, repl={}, result={:?}",
            self, nodeid, uuid, repl, r
        );
        if !r.is_err() && repl {
            server
                .repl_backlog
                .replicate_cmd(uuid, self.command.name, self.args.clone());
        }
        r
    }
}

type CommandHandler = fn(
    server: &mut Server,
    client: Option<&mut Client>,
    nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError>;

pub struct Command {
    name: &'static str,
    handler: CommandHandler,
    flags: u16,
}

impl Debug for Command {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Command{{name: {}}}", self.name))
    }
}

pub const COMMAND_READONLY: u16 = 1;
pub const COMMAND_WRITE: u16 = 1 << 1;
pub const COMMAND_CTRL: u16 = 1 << 2;
pub const COMMAND_NO_REPLICATE: u16 = 1 << 3;
#[allow(unused)]
pub const COMAND_NO_REPLY: u16 = 1 << 4;
pub const COMMAND_REPL_ONLY: u16 = 1 << 5;

macro_rules! new_command {
    ($table:expr, $name:expr, $handler: expr, $flags:expr) => {
        $table.insert(
            $name.as_bytes(),
            Command {
                name: $name,
                handler: $handler,
                flags: $flags,
            },
        );
    };
}

lazy_static! {
    pub static ref COMMANDS: HashMap<&'static [u8], Command> = {
        let mut command_table = HashMap::new();
        // control
        new_command!(command_table, "node", node_command, COMMAND_CTRL);
        new_command!(command_table, "replicas", replicas_command, COMMAND_READONLY);
        new_command!(command_table, "sync", sync_command, COMMAND_CTRL);
        new_command!(command_table, "meet", meet_command, COMMAND_CTRL);
        new_command!(command_table, "forget", forget_command, COMMAND_WRITE);

        //client
        new_command!(command_table, "wait", wait_command, COMMAND_WRITE);
        new_command!(command_table, "client", client_command, COMMAND_CTRL);

        //stats
        new_command!(command_table, "repllog", repllog_command, COMMAND_READONLY);
        new_command!(command_table, "replcheck", replcheck_command, COMMAND_READONLY);
        new_command!(command_table, "info", info_command, COMMAND_READONLY);

        // common commands
        new_command!(command_table, "get", get_command, COMMAND_READONLY);
        new_command!(command_table, "set", set_command, COMMAND_WRITE);
        new_command!(command_table, "desc", desc_command, COMMAND_READONLY);
        new_command!(command_table, "del", del_command, COMMAND_WRITE | COMMAND_NO_REPLICATE);
        new_command!(command_table, "delbytes", delbytes_command, COMMAND_WRITE | COMMAND_REPL_ONLY);

        // multi-version
        new_command!(command_table, "mvget", mvget_command, COMMAND_READONLY);
        new_command!(command_table, "mvset", mvset_command, COMMAND_WRITE);
        new_command!(command_table, "delmvreg", delmvreg_command, COMMAND_WRITE | COMMAND_REPL_ONLY);

        // counter
        new_command!(command_table, "incr", incr_command, COMMAND_WRITE);
        new_command!(command_table, "decr", decr_command, COMMAND_WRITE);
        new_command!(command_table, "delcnt", delcnt_command, COMMAND_WRITE | COMMAND_REPL_ONLY);


        // set
        new_command!(command_table, "sadd", sadd_command, COMMAND_WRITE);
        new_command!(command_table, "srem", srem_command, COMMAND_WRITE);
        new_command!(command_table, "spop", spop_command, COMMAND_WRITE);
        new_command!(command_table, "smembers", smembers_command, COMMAND_READONLY);
        new_command!(command_table, "delset", delset_command, COMMAND_WRITE | COMMAND_REPL_ONLY);


        // dict
        new_command!(command_table, "hset", hset_command, COMMAND_WRITE);
        new_command!(command_table, "hget", hget_command, COMMAND_READONLY);
        new_command!(command_table, "hgetall", hgetall_command, COMMAND_READONLY);
        new_command!(command_table, "hdel", hdel_command, COMMAND_WRITE);
        new_command!(command_table, "deldict", deldict_command, COMMAND_WRITE | COMMAND_REPL_ONLY);

        // list
        new_command!(command_table, "llen", llen_command, COMMAND_READONLY);
        new_command!(command_table, "lindex", lindex_command, COMMAND_READONLY);
        new_command!(command_table, "lrange", lrange_command, COMMAND_READONLY);
        new_command!(command_table, "lpos", lpos_command, COMMAND_READONLY);
        new_command!(command_table, "dellist", dellist_command, COMMAND_WRITE | COMMAND_REPL_ONLY);
        new_command!(command_table, "rpop", rpop_command, COMMAND_WRITE | COMMAND_NO_REPLICATE);
        new_command!(command_table, "rpush", rpush_command, COMMAND_WRITE | COMMAND_NO_REPLICATE);
        new_command!(command_table, "lpop", lpop_command, COMMAND_WRITE | COMMAND_NO_REPLICATE);
        new_command!(command_table, "lpush", lpush_command, COMMAND_WRITE | COMMAND_NO_REPLICATE);
        new_command!(command_table, "lrem", lrem_command, COMMAND_WRITE | COMMAND_NO_REPLICATE);
        new_command!(command_table, "lset", lset_command, COMMAND_WRITE | COMMAND_NO_REPLICATE);
        new_command!(command_table, "linsert", linsert_command, COMMAND_WRITE | COMMAND_NO_REPLICATE);
        new_command!(command_table, "lchangeat", lchangeat_command, COMMAND_WRITE | COMMAND_NO_REPLICATE | COMMAND_REPL_ONLY);
        new_command!(command_table, "ltrim", ltrim_command, COMMAND_WRITE | COMMAND_NO_REPLICATE);


        command_table
    };
}

pub fn node_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    _uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let c_type = args.next_bytes()?;
    let v = args.next_bytes();
    match (c_type.as_bytes(), v) {
        (b"id", Err(_)) => Ok(Message::Integer(server.node_id as i64)),
        (b"id", Ok(r)) => {
            if let Some(i) = bytes2i64(r.as_bytes()) {
                if i > 0 {
                    server.node_id = i as u64;
                    return Ok(new_msg_ok());
                }
            }
            Ok(Message::Error("id must be greater than 0".into()))
        }
        (b"alias", Err(_)) => Ok(Message::BulkString(server.node_alias.as_bytes().into())),
        (b"alias", Ok(r)) => {
            server.node_alias = String::from(r);
            Ok(new_msg_ok())
        }
        _ => Ok(Message::Error("unsupported command".into())),
    }
}

pub fn get_command(
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
            match &o.enc {
                Encoding::Counter(c) => Ok(Message::Integer(c.get())),
                Encoding::Bytes(b) => Ok(Message::BulkString(b.clone())),
                _ => Err(CstError::InvalidType),
            }
        }
        None => Ok(Message::Nil),
    }
}

pub fn set_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let value = args.next_bytes()?;
    // let o = server.db.entry(key_name).or_insert(Object::new(Encoding::Bytes(value.clone()), uuid, 0));
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::Bytes(value.clone()), uuid, 0);
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    if o.update_time > uuid {
        return Ok(Message::Integer(0));
    }
    match o.enc {
        Encoding::Bytes(_) => {}
        _ => return Err(CstError::InvalidType),
    }
    o.enc = Encoding::Bytes(value);
    o.updated_at(uuid);
    Ok(new_msg_ok())
}

pub fn desc_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    match server.db.query(&key_name, uuid) {
        None => Ok(Message::Nil),
        Some(o) => Ok(o.describe()),
    }
}

pub fn del_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let mut deleted = 0;
    let mut replicates = vec![];
    let key_name = args.next_bytes()?;
    match server.db.query(&key_name, uuid) {
        None => {}
        Some(v) => {
            debug!(
                "deleting object, ct: {}, dt: {}, mt: {}",
                v.create_time, v.delete_time, v.update_time
            );
            if v.delete_at(uuid) {
                deleted = 1;
            }
            match &mut v.enc {
                Encoding::Counter(g) => {
                    let mut d = HashMap::new();
                    for (nodeid, (value, _)) in g.iter() {
                        d.insert(nodeid, value);
                    }
                    let mut args = Vec::with_capacity(d.len() * 2 + 1);
                    args.push(Message::BulkString(key_name.into()));
                    for (nodeid, value) in d {
                        g.change(nodeid, -value, uuid);
                        args.push(Message::Integer(nodeid as i64));
                        args.push(Message::Integer(-value));
                    }
                    replicates.push(("delcnt", args));
                }
                Encoding::Bytes(_) => {
                    replicates.push(("delbytes", vec![Message::BulkString(key_name.into())]));
                }
                Encoding::LWWSet(s) => {
                    let members: Vec<Bytes> = s.iter_all().map(|(x, _)| x.clone()).collect();
                    let _ = s.remove_members(members.as_slice(), uuid);
                    replicates.push(("delset", vec![Message::BulkString(key_name.into())]));
                }
                Encoding::LWWDict(d) => {
                    let fields: Vec<Bytes> = d.iter_all().map(|(b, _, _)| b.clone()).collect();
                    let _ = d.del_fields(fields.as_slice(), uuid);
                    v.delete_time = max(v.delete_time, uuid);
                    v.update_time = max(v.update_time, uuid);
                    replicates.push(("deldict", vec![Message::BulkString(key_name.into())]));
                }
                Encoding::List(l) => {
                    let mut args: Vec<Message> = Vec::with_capacity(l.len() * 3 + 1);
                    args.push(Message::BulkString(key_name.into()));
                    for (p, _) in l.iter() {
                        args.push(Message::BulkString(p.number().marshal().into()));
                        args.push(Message::Integer(p.uuid() as i64));
                        args.push(Message::Integer(p.nodeid() as i64));
                    }
                    v.delete_time = max(v.delete_time, uuid);
                    v.update_time = max(v.update_time, uuid);
                    replicates.push(("dellist", args));
                }
                Encoding::MVREG(r) => {
                    replicates.push(("delmvreg", vec![
                        Message::BulkString(key_name.into()),
                        Message::Integer(nodeid as i64),
                    ]));
                    deleted = if r.del(nodeid, uuid) { 1 } else { 0 };
                    v.update_time = max(v.update_time, uuid);
                }
            }
        }
    }

    for (cmd, args) in replicates {
        server.repl_backlog.replicate_cmd(uuid, cmd, args);
    }
    Ok(Message::Integer(deleted))
}

pub fn delbytes_command(
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
            let o = Object::new(Encoding::Bytes("".into()), uuid, 0);
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    match o.enc {
        Encoding::Bytes(_) => {}
        _ => return Err(CstError::InvalidType),
    }
    o.delete_time = max(o.delete_time, uuid);
    o.update_time = max(o.update_time, uuid);
    Ok(Message::None)
}

pub fn repllog_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    _uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let sub_command = args.next_string()?;
    match sub_command.to_ascii_lowercase().as_str() {
        "at" => {
            let uuid = args.next_u64()?;
            Ok(server.repl_backlog.log_at(uuid).unwrap_or(Message::Nil))
        }
        "uuids" => {
            let uuids: Vec<Message> = server
                .repl_backlog
                .log_uuids()
                .into_iter()
                .map(|x| Message::Integer(x as i64))
                .collect();
            Ok(Message::Array(uuids))
        }
        others => Err(CstError::UnknownSubCmd(
            others.to_string(),
            "REPLLOG".to_string(),
        )),
    }
}

pub fn replcheck_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    _uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let node_addr = args.next_string()?;
    let node_uuid = args.next_u64()?;
    let mut r = -1;
    server.replicas.iter(|x| {
        if x.he.addr == node_addr {
            if x.uuid_he_acked <= node_uuid {
                r = 1;
            } else {
                r = 0;
            }
        }
    });
    Ok(Message::Integer(r))
}

pub trait NextArg {
    fn next_arg(&mut self) -> Result<Message, CstError>;
    fn next_bytes(&mut self) -> Result<Bytes, CstError>;
    fn next_i64(&mut self) -> Result<i64, CstError>;
    fn next_u64(&mut self) -> Result<u64, CstError>;
    fn next_string(&mut self) -> Result<String, CstError>;
}

impl<T> NextArg for T
where
    T: Iterator<Item = Message>,
{
    fn next_arg(&mut self) -> Result<Message, CstError> {
        self.next().map_or(Err(CstError::WrongArity), |x| Ok(x))
    }

    fn next_bytes(&mut self) -> Result<Bytes, CstError> {
        match self.next_arg()? {
            Message::Integer(i) => Ok(get_int_bytes(i)),
            Message::Error(e) => Ok(e),
            Message::String(s) => Ok(s),
            Message::BulkString(b) => Ok(b),
            _ => Err(CstError::InvalidRequestMsg(
                "should be non-array type".to_string(),
            )),
        }
    }

    fn next_i64(&mut self) -> Result<i64, CstError> {
        match self.next_arg()? {
            Message::Integer(i) => Ok(i),
            Message::String(s) => bytes2i64(s.as_bytes()).ok_or(CstError::InvalidRequestMsg(
                "string should be an integer".to_string(),
            )),
            Message::BulkString(s) => bytes2i64(s.as_bytes()).ok_or(CstError::InvalidRequestMsg(
                "bulk string should be an integer".to_string(),
            )),
            _ => Err(CstError::InvalidRequestMsg(
                "argument should be of type Integer or String or BulkString".to_string(),
            )),
        }
    }

    fn next_u64(&mut self) -> Result<u64, CstError> {
        match self.next_i64() {
            Ok(i) => {
                if i >= 0 {
                    Ok(i as u64)
                } else {
                    Err(CstError::InvalidRequestMsg(
                        "argument should be an unsigned integer".to_string(),
                    ))
                }
            }
            Err(e) => Err(e),
        }
    }

    fn next_string(&mut self) -> Result<String, CstError> {
        self.next_bytes().map(|x| x.into())
    }
}
