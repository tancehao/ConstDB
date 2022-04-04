use crate::cmd::NextArg;
use crate::conn::Conn;
use crate::crdt::lwwhash::LWWHash;
use crate::link::{Link, LinkType};
use crate::replica::pull::{PullStat, Puller};
use crate::replica::push::{PushStat, Pusher};
use crate::resp::Message;
use crate::server::{EventsConsumer, Server};
use crate::snapshot::{SnapshotWriter, SNAPSHOT_FLAG_REPLICA_ADD, SNAPSHOT_FLAG_REPLICA_REM};
use crate::CstError;
use std::collections::HashMap;
use std::io::Write;
use tokio::net::TcpSocket;
use tokio::time::{sleep, Duration};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub struct ReplicaManager {
    myself: ReplicaIdentity,
    replicas: LWWHash<String, ReplicaMeta>,
}

impl ReplicaManager {
    pub fn new(myself: ReplicaIdentity) -> Self {
        Self {
            myself,
            replicas: LWWHash::empty(),
        }
    }

    pub fn add_replica(&mut self, addr: String, meta: ReplicaMeta, t: u64) -> bool {
        debug!("Adding a replica, addr={}", addr);
        self.replicas.set(addr, meta, t)
    }

    pub fn iter<F>(&self, mut f: F)
        where F: FnMut(&ReplicaMeta) -> (),
    {
        self.replicas.add.iter().for_each(|(_, (_, x))| f(x));
    }

    pub fn remove_replica(&mut self, addr: &String, t: u64) -> bool {
        self.replicas.rem(addr, t)
    }

    pub fn update_replica_pull_stat(
        &mut self,
        id: &ReplicaIdentity,
        uuid_he_sent: u64,
        uuid_he_acked: u64,
    ) {
        match self.replicas.get_mut(&id.addr) {
            None => {}
            Some(o) => {
                o.uuid_he_sent = uuid_he_sent;
                o.uuid_he_acked = uuid_he_acked;
            }
        }
    }

    pub fn update_replica_push_stat(
        &mut self,
        id: &ReplicaIdentity,
        uuid_i_sent: u64,
        uuid_i_acked: u64,
    ) {
        match self.replicas.get_mut(&id.addr) {
            None => {}
            Some(o) => {
                o.uuid_i_sent = uuid_i_sent;
                o.uuid_i_acked = uuid_i_acked;
            }
        }
    }

    pub fn update_replica_identity(&mut self, id: &ReplicaIdentity) {
        if let Some(r) = self.replicas.get_mut(&id.addr) {
            r.he = id.clone();
        }
    }

    pub fn generate_replicas_reply(&self, current_uuid: u64) -> Message {
        let replicas = {
            let replica_cnt = self.replicas.add.len();
            let mut replicas = Vec::with_capacity(replica_cnt + 1);
            replicas.push(Message::Array(vec![
                Message::BulkString(self.myself.alias.clone().into()),
                Message::Integer(self.myself.id as i64),
                Message::BulkString(self.myself.addr.clone().into()),
                Message::Integer(current_uuid as i64),
            ]));
            for (_, (_, r)) in self.replicas.add.iter() {
                let replica_info = vec![
                    Message::BulkString(r.he.alias.clone().into()),
                    Message::Integer(r.he.id as i64),
                    Message::BulkString(r.he.addr.clone().into()),
                    Message::Integer(r.uuid_he_sent as i64),
                ];
                replicas.push(Message::Array(replica_info));
            }
            replicas
        };
        Message::Array(replicas)
    }

    pub fn min_uuid(&self) -> Option<u64> {
        self.replicas
            .add
            .iter()
            .map(|(_, (_, id))| id.uuid_he_sent)
            .min()
    }

    #[inline]
    pub fn has_replica(&self, addr: &String) -> bool {
        self.replicas.get(addr).is_some()
    }

    pub fn replica_forgotten(&self, addr: &String) -> bool {
        self.replicas.removed(addr)
    }

    pub fn dump_snapshot<T: Write>(&self, w: &mut SnapshotWriter<T>) -> Result<(), CstError> {
        for (_, (t, meta)) in self.replicas.add.iter() {
            w.write_byte(SNAPSHOT_FLAG_REPLICA_ADD)?
                .write_integer(*t as i64)?
                .write_integer(meta.he.id as i64)?
                .write_integer(meta.he.alias.len() as i64)?
                .write_bytes(meta.he.alias.as_bytes())?
                .write_integer(meta.he.addr.len() as i64)?
                .write_bytes(meta.he.addr.as_ref())?
                .write_integer(meta.uuid_he_sent as i64)?;
        }

        for (addr, t) in self.replicas.del.iter() {
            w.write_byte(SNAPSHOT_FLAG_REPLICA_REM)?
                .write_integer(addr.len() as i64)?
                .write_bytes(addr.as_bytes())?
                .write_integer(*t as i64)?;
        }
        Ok(())
    }

    pub fn replica_progress(&self) -> HashMap<String, u64> {
        let mut r = HashMap::with_capacity(self.replicas.add.len());
        for (_, (_, v)) in self.replicas.add.iter() {
            r.insert(v.he.addr.clone(), v.uuid_he_sent);
        }
        r
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaMeta {
    pub uuid_i_sent: u64,
    pub uuid_he_acked: u64,

    pub uuid_he_sent: u64,
    pub uuid_i_acked: u64,

    pub myself: ReplicaIdentity,
    pub he: ReplicaIdentity,

    pub status: &'static str,

    pub uuid_he_sent_last_dump: u64,

    pub latest_acked_time: u64,
    pub close: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ReplicaIdentity {
    pub id: u64,
    pub addr: String,
    pub alias: String,
}
#[derive(Debug)]
pub struct Replica {
    pub meta: ReplicaMeta,
    pub conn: Conn,
    pub(crate) stat: ReplicaStat,
    pub(crate) events: Option<EventsConsumer<u64>>,
    to_serve: bool,
    to_close: bool,
}

#[derive(Debug)]
pub enum ReplicaStat {
    NotConnected,
    // send sync with my infos and sync positions.
    // the remote replica replies with his infos and sync positions
    Handshake(Conn, bool),
    // exchange commands
    Alive(Puller, Pusher),
}

#[async_trait]
impl Link for Replica {
    fn addr(&self) -> &str {
        self.conn.addr.as_str()
    }

    fn link_type(&self) -> LinkType {
        LinkType::Replica
    }

    fn serve(&mut self, server: &mut Server) {
        if self.interact_in_main(server).is_err() {
            self.to_close = true;
        }
    }

    async fn prepare(&mut self) {
        if let Err(e) = self.interact_independently().await {
            error!(
                "Failed to interact with replica at {}, err={}",
                self.meta.he.addr, e
            );
            self.stat = ReplicaStat::NotConnected;
        }
    }

    #[inline]
    fn to_serve(&self) -> bool {
        self.to_serve
    }

    #[inline]
    fn to_close(&self) -> bool {
        self.to_close
    }

    async fn close(&mut self) -> Result<(), CstError> {
        Ok(())
    }
}

impl Replica {
    pub fn new(his_addr: String, my_id: u64, my_alias: String, my_addr: String) -> Self {
        Self {
            meta: ReplicaMeta {
                myself: ReplicaIdentity {
                    id: my_id,
                    alias: my_alias,
                    addr: my_addr,
                },
                he: ReplicaIdentity {
                    id: 0,
                    alias: "".to_string(),
                    addr: his_addr.clone(),
                },
                uuid_i_sent: 0,
                uuid_he_acked: 0,
                uuid_he_sent: 0,
                uuid_i_acked: 0,
                uuid_he_sent_last_dump: 0,
                close: false,
                latest_acked_time: 0,
                status: "",
            },
            stat: ReplicaStat::NotConnected,
            conn: Conn::new(None, his_addr),
            events: None,
            to_serve: false,
            to_close: false,
        }
    }

    pub fn interact_in_main(&mut self, server: &mut Server) -> Result<(), CstError> {
        match &mut self.stat {
            ReplicaStat::Alive(puller, pusher) => {
                if server.replicas.replica_forgotten(&self.meta.he.addr) {
                    info!(
                        "The replica at {} is forgotten from the cluster",
                        self.meta.he.addr
                    );
                    pusher.writer.write_msg(Message::Error(
                        "Stop replication because you're removed from the cluster".into(),
                    ));
                    self.to_close = true;
                } else {
                    puller.merge_replicates_in_main(server)?;
                    pusher.push_to_replica_in_main(server, puller.uuid_he_sent)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub async fn interact_independently(&mut self) -> Result<(), CstError> {
        self.to_serve = false;
        loop {
            match &mut self.stat {
                ReplicaStat::NotConnected => {
                    debug!(
                        "The replica at {} is in NotConnected stat",
                        self.meta.he.addr
                    );
                    let _ = std::mem::replace(
                        &mut self.conn,
                        Conn::new(None, self.meta.he.addr.clone()),
                    );
                    let socket = TcpSocket::new_v4()?;
                    socket.set_reuseaddr(true)?;
                    socket.set_reuseport(true)?;
                    socket.bind(self.meta.myself.addr.parse().unwrap())?;
                    match socket.connect(self.meta.he.addr.parse().unwrap()).await {
                        Err(e) => {
                            error!("Failed to connect to the replica at {} because {}, wait and try again later.", self.meta.he.addr, e);
                            sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                        Ok(c) => {
                            let _ = c.set_nodelay(true);
                            info!("Connected to replica at {}", self.meta.he.addr);
                            self.stat = ReplicaStat::Handshake(
                                Conn::new(Some(c), self.meta.he.addr.clone()),
                                false,
                            );
                        }
                    }
                }
                ReplicaStat::Handshake(conn, passive) => {
                    debug!("Replica at {} is in Handshake stat", self.meta.he.addr);
                    if !*passive {
                        // send the sync command and wait for his response
                        conn.send_msg(mkcmd!(
                            "SYNC",
                            0,
                            self.meta.myself.id,
                            self.meta.myself.alias,
                            self.meta.uuid_he_sent
                        ))
                        .await?;
                        let mut args = match conn.next_msg().await? {
                            Message::Array(args) => args.into_iter(),
                            others => {
                                error!("Unexpected response for SYNC request, got {:?}", others);
                                return Err(CstError::InvalidType);
                            }
                        };
                        let _ = args.next_string()?; // SYNC
                        let _ = args.next_u64()?; // 1
                        let (his_id, his_alias, uuid_i_sent) =
                            (args.next_u64()?, args.next_string()?, args.next_u64()?);
                        self.meta.he.id = his_id;
                        self.meta.he.alias = his_alias;
                        self.meta.uuid_i_sent = uuid_i_sent;
                    } else {
                        // we've already received his sync command, send our response and start to exchange dataset.
                        let meta = &self.meta;
                        conn.send_msg(mkcmd!(
                            "SYNC",
                            1,
                            meta.myself.id,
                            meta.myself.alias,
                            meta.uuid_he_sent
                        ))
                        .await?;
                    }
                    let (reader, writer) = conn.split();
                    let urged = Arc::new(AtomicBool::new(false));
                    let puller = Puller {
                        uuid_he_sent: self.meta.uuid_he_sent,
                        uuid_he_acked: self.meta.uuid_he_acked,
                        meta: self.meta.clone(),
                        stats: PullStat::SyncSent,
                        reader,
                        snapshot_entries: Default::default(),
                        replicates: Default::default(),
                        urged: urged.clone(),
                    };
                    let events = self.events.take().ok_or(CstError::SystemError)?;
                    let pusher = Pusher {
                        uuid_i_sent: self.meta.uuid_i_sent,
                        uuid_i_acked: self.meta.uuid_i_acked,
                        latest_ack_time: 0,
                        meta: self.meta.clone(),
                        stats: PushStat::SyncReceived,
                        writer,
                        events,
                        urged: urged,
                    };
                    self.stat = ReplicaStat::Alive(puller, pusher);
                }
                ReplicaStat::Alive(puller, pusher) => {
                    debug!("Replica at {} is in alive stat", self.meta.he.addr);
                    match pusher.stats {
                        PushStat::SyncReceived => {
                            self.to_serve = true;
                            return Ok(());
                        }
                        _ => {}
                    }
                    let (mut pull_result, mut push_result) = (Ok(()), Ok(()));
                    match (&puller.stats, &pusher.stats) {
                        (PullStat::PullingCommands, PushStat::PushingCommands) => {
                            tokio::select! {
                                r = puller.accept_replicates() => pull_result = r,
                                t = pusher.watching_my_replog() => push_result = t,
                            }
                        }
                        _ => {
                            let (r, t) =
                                tokio::join!(puller.download_snapshot(), pusher.sending_snapshot());
                            pull_result = r;
                            push_result = t;
                        }
                    }
                    if let Err(e) = pull_result {
                        error!(
                            "Error occured when pulling data from the replica at {}, err={}",
                            self.meta.he.addr, e
                        );
                        self.meta.close = true;
                        return Err(CstError::SystemError);
                    }
                    if let Err(e) = push_result {
                        error!(
                            "Error occured when pushing data to the replica at {}, err={}",
                            self.meta.he.addr, e
                        );
                        self.meta.close = true;
                        return Err(CstError::SystemError);
                    }
                    self.to_serve = true;
                    return Ok(());
                }
            }
        }
    }
}
