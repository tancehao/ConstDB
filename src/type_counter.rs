use crate::client::Client;
use crate::cmd::NextArg;
use crate::crdt::vclock::Counter;
use crate::object::{Encoding, Object};
use crate::resp::Message;
use crate::server::Server;
use crate::CstError;

pub fn delcnt_command(
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
            let o = Object::new(Encoding::from(Counter::default()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    o.delete_at(uuid);
    match &mut o.enc {
        Encoding::Counter(c) => {
            while let Ok(nodeid) = args.next_u64() {
                let v = args.next_i64()?;
                c.change(nodeid, v, uuid);
            }
            Ok(Message::None)
        }
        _ => return Err(CstError::InvalidType),
    }
}

pub fn incr_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
    let o = match server.db.query(&key_name, uuid) {
        None => {
            let o = Object::new(Encoding::from(Counter::default()), uuid, 0).into();
            server.db.add(key_name.clone(), o);
            server.db.query(&key_name, uuid).unwrap()
        }
        Some(o) => o,
    };
    let c = o.enc.as_mut_counter()?;
    let v = c.change(nodeid, 1, uuid);
    o.updated_at(uuid);
    Ok(Message::Integer(v))
}

pub fn decr_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let key_name = args.next_bytes()?;
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
