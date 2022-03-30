pub mod pull;
pub mod push;
pub mod replica;

use std::net::SocketAddr;

use crate::cmd::NextArg;
use crate::link::{Client, Link, SharedLink};
use crate::replica::replica::{Replica, ReplicaStat};
use crate::resp::Message;
use crate::server::Server;
use crate::CstError;

pub fn sync_command(
    server: &mut Server,
    client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let client = client.unwrap();
    let addr = client.addr().to_string();
    let mut args = args.into_iter();
    let _ = args.next_u64()?; // zero meaning that the client is the one requesting a sync
    let nodeid = args.next_u64()?;
    let his_alias = args.next_string()?;
    let uuid_i_sent = args.next_u64()?;
    let mut replica = Replica::new(
        addr.clone(),
        server.node_id,
        server.config.node_alias.clone(),
        format!("{}:{}", server.config.ip, server.config.port),
    );
    replica.meta.he.id = nodeid;
    replica.meta.he.alias = his_alias;
    replica.meta.he.addr = addr.clone();
    replica.meta.uuid_i_sent = uuid_i_sent;
    let conn = client.take_conn();
    replica.stat = ReplicaStat::Handshake(conn, true);
    replica.events = Some(server.repl_backlog.new_watcher(0));
    server
        .replicas
        .add_replica(addr, replica.meta.clone(), uuid);

    let mut sl = SharedLink::from(replica);
    tokio::spawn(async move {
        sl.prepare().await;
    });
    client.close = true;
    Ok(Message::None)
}

// MEET command is sent to a new node, with the address of an existing node which is already a member of a multi-active cluster.
// Then the node received the MEET command asynchronously connects to that node and then initiate a handshake: it sends a SYNC
// command with it's identities and previous synchronization progress to that node, and the later response with a SYNC command
// too. Soon later, both nodes start to exchange their own data with each other - firstly dump a snapshot if it's impossible to
// continue from a checkpoint, and then send their write commands to each other. They also exchange their knowledge about other
// existing replicas of their own, and connects to them that they've never know and keep pace with them. This way two group of
// replicas are merged into one.
pub fn meet_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    if server.node_id == 0 || server.node_alias.is_empty() {
        return Ok(Message::Error(
            "Should set my node_id and node_alias first".into(),
        ));
    }
    let mut args = args.into_iter();
    match args
        .next_string()?
        .parse::<SocketAddr>()
        .map(|x| x.to_string())
    {
        Ok(addr) => {
            let mut r = Replica::new(
                addr.clone(),
                server.node_id,
                server.config.node_alias.clone(),
                server.addr.clone(),
            );
            r.meta.uuid_i_sent = server.repl_backlog.last_uuid();
            r.events = Some(server.repl_backlog.new_watcher(0));

            let i = if server
                .replicas
                .add_replica(addr.clone(), r.meta.clone(), uuid)
            {
                1
            } else {
                0
            };

            let mut sl = SharedLink::from(r);
            tokio::spawn(async move {
                sl.prepare().await;
            });
            Ok(Message::Integer(i))
        }
        Err(_) => Ok(Message::Error("invalid socket address".into())),
    }
}

pub fn forget_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    let addr_to_forget = args.next_string()?;
    let r = if server.replicas.remove_replica(&addr_to_forget, uuid) {
        1
    } else {
        0
    };
    Ok(Message::Integer(r))
}

// fetch the whole replica set the current node is communicating with.
// used when a new node is joining the cooperate group.
pub fn replicas_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    uuid: u64,
    _args: Vec<Message>,
) -> Result<Message, CstError> {
    Ok(server.replicas.generate_replicas_reply(uuid))
}
