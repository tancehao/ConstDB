use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::ops::Deref;
use std::rc::Rc;
use std::result::Result::Ok;

use bitflags::_core::option::Option::Some;
use bitflags::_core::time::Duration;
use nix::unistd::{fork, ForkResult, Pid};
use tokio::net::TcpSocket;
use tokio::sync::OwnedMutexGuard;
use tokio::task::spawn_local;
use tokio::time::Instant;
use tokio::time::interval_at;

use crate::{Bytes, CstError, now_mil};
use crate::conf::Config;
use crate::db::DB;
use crate::link::{Client, Link, SharedLink};
use crate::object::Object;
use crate::replica::replica::{ReplicaIdentity, ReplicaManager};
use crate::resp::Message;
use crate::snapshot::{SNAPSHOT_FLAG_CHECKSUM, SnapshotWriter};
use crate::stats::{incr_clients, Metrics};

pub struct Server {
    pub config: &'static Config,
    pub addr: String,
    pub node_id: u64,
    pub node_alias: String,
    uuid: u64,
    pub expires: HashMap<Bytes, Object>,
    pub db: DB,
    repl_log: VecDeque<(u64, &'static str, Vec<Message>)>,
    latest_repl_uuid_overflowed: Option<u64>,
    repl_log_size: u64,
    repl_log_size_limit: u64,

    pub replicas: ReplicaManager,
    // replicas: LWWHash<u64, ReplicaIdentity>,
    pub events: EventsProducer,
    #[allow(unused)]
    events_wather: EventsConsumer,

    // latest time a snapshot was dumped, and the replica ids and their uuids we received at that time
    snapshot: (u64, HashMap<String, u64>),

    latest_dump_time: u64,
    latest_dumped_at_uuid: u64,
    pub client_chan: tokio::sync::mpsc::Sender<OwnedMutexGuard<Box<dyn Link + Send>>>,
    pub metrics: Metrics,
}

pub enum ServerEvent {
    Started,
    Replicated,
}

impl Server {
    pub fn new(config: &'static Config) -> Self {
        let (tx, rx) = new_events_chann();
        let (c_tx, _) = tokio::sync::mpsc::channel(1);
        let identity = ReplicaIdentity{
            id: config.node_id,
            addr: config.addr.clone(),
            alias: config.node_alias.clone(),
        };

        Server {
            node_id: config.node_id,
            node_alias: config.node_alias.clone(),
            addr: config.addr.clone(),
            config,
            uuid: 1,
            expires: HashMap::new(),
            db: DB::empty(),
            repl_log: VecDeque::with_capacity(1024),
            latest_repl_uuid_overflowed: None,
            repl_log_size: 0,
            repl_log_size_limit: 1024000,
            events: tx,
            events_wather: rx,
            //replicas: HashMap::new(),
            replicas: ReplicaManager::new(identity),
            snapshot: (0, Default::default()),
            latest_dump_time: 0,
            latest_dumped_at_uuid: 0,
            client_chan: c_tx,
            metrics: Default::default(),
        }
    }

    pub async fn run(c: &'static Config) -> Result<(), std::io::Error> {
        let server = Rc::new(RefCell::new(Server::new(c)));
        let addr = format!("{}:{}", c.ip, c.port).parse::<SocketAddr>().unwrap();
        let socket = TcpSocket::new_v4()?;
        socket.set_reuseaddr(true)?;
        socket.set_reuseport(true)?;
        socket.bind(addr)?;
        let listener = socket.listen(c.tcp_backlog)?;
        let server_c = server.clone();
        let server_cc = server.clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel(102400);
        server.deref().borrow_mut().client_chan = tx.clone();
        spawn_local(async move {
            loop {
                match listener.accept().await {
                    Err(e) => {
                        error!("failed to accept new connection because {}", e);
                        std::process::exit(-1);
                    }
                    Ok((conn, peer_addr)) => {
                        incr_clients();
                        let ec = Server::new_event_consumer(server_cc.clone());
                        let mut sc = SharedLink::from(Client::new(conn, peer_addr.to_string(), ec));
                        let tx_c = tx.clone();
                        tokio::task::spawn(async move {
                            sc.prepare(tx_c).await;
                        });
                    }
                }
            }
        });
        spawn_local(async move {
            Self::cron(server_c).await;
        });
        while let Some(mut l) = rx.recv().await {
            l.serve( &mut *(*server).borrow_mut());
        }
        Ok(())
    }

    async fn cron(server: Rc<RefCell<Server>>) {
        let mut timer = interval_at(Instant::now() + Duration::from_secs(1), Duration::from_millis(100));
        loop {
            {
                let mut s = server.deref().borrow_mut();
                let _ = s.next_uuid(true);
            }
            let _ = timer.tick().await;
            server.deref().borrow_mut().gc();
            // check for new replicas
            let _ = server.clone();
        }
    }

    fn new_event_consumer(server: Rc<RefCell<Server>>) -> EventsConsumer {
        let e = server.deref().borrow().events.events.subscribe();
        EventsConsumer{
            watching: 0,
            events: e,
        }
    }

    // generate a uuid that is associated with the command currently being executed
    // this uuid is also used as a timestamp.
    // for writing,  we always return a bigger uuid.
    pub fn next_uuid(&mut self, is_write: bool) -> u64 {
        let (time_mil, mut sequnce) = (self.uuid >> 22, (self.uuid & ((1<<22)-1)));
        let now = now_mil();
        self.uuid = {
            if is_write {
                if time_mil == now {
                    sequnce += 1;
                } else {
                    sequnce = 0;
                }
            }
            now << 22 | sequnce
        };
        self.uuid
    }

    pub fn current_uuid(&self) -> u64 {
        self.uuid
    }

    pub fn current_time(&self) -> u64 {
        self.uuid >> 22
    }

    pub fn dump_all(&mut self, file_name: String) -> Result<(), CstError> {
        debug!("begin to dump, current_dir is {:?}", std::env::current_dir());
        let tmp_name = format!("snapshot_{}", chrono::Local::now().timestamp());
        let f = std::fs::OpenOptions::new().create(true).write(true).truncate(true).open(tmp_name.clone())?;
        debug!("tmp file {} created", tmp_name);

        let mut w = SnapshotWriter::new(4096, f);
        w.write_bytes(b"CONSTDB")?;
        w.write_bytes([0u8, 1, 1, 1].as_ref())?;

        // dump my metadatas
        let _ = w.write_integer(self.node_id as i64)?
            .write_integer(self.node_alias.len() as i64)?
            .write_bytes(self.node_alias.as_bytes())?
            .write_integer(self.addr.len() as i64)?
            .write_bytes(self.addr.as_ref())?
            .write_integer(self.get_repl_last_uuid() as i64)?;

        // dump the db
        self.db.dump(&mut w)?;

        self.replicas.dump_snapshot(&mut w)?;
        w.write_byte(SNAPSHOT_FLAG_CHECKSUM)?;
        let checksum = w.checksum();
        w.write_bytes(checksum.to_le_bytes().as_ref())?;
        w.flush()?;
        debug!("dump finished");
        self.latest_dumped_at_uuid = self.current_uuid();
        self.latest_dump_time = chrono::Local::now().timestamp() as u64;
        std::fs::rename(tmp_name, file_name.clone())?;
        debug!("temp_file was renamed to {}", file_name);
        Ok(())
    }

    pub fn get_max_uuid_dumped(&self) -> u64 {
        self.latest_dumped_at_uuid
    }

    pub fn dump_snapshot_in_background(&mut self) -> Result<(Option<Pid>, String, u64), CstError> {
        // check for the latest time we've dumped a snapshot
        debug!("dumping snapshot in background");
        let file_name = "db.snapshot".to_string();
        let pid = if self.snapshot.0 > self.get_repl_first_uuid() {  // Congratulations! we've dumped a snapshot not long before, so we can use that snapshot
            debug!("we've dumped a snapshot not long before, we can use that one!");
            None
        } else {  // we need to dump a fresh snapshot now!
            match unsafe { fork() } {
                Ok(ForkResult::Child) => {
                    if let Err(e) = self.dump_all(file_name.clone()) {
                        error!("unable to dump a snapshot because {}", e);
                    }
                    std::process::exit(0);
                }
                Ok(ForkResult::Parent { child: pid }) => {
                    debug!("forked a child process {}", pid);
                    let mut tombstones = self.replicas.replica_progress();
                    tombstones.insert(self.addr.clone(), self.get_repl_last_uuid());
                    self.snapshot = (self.get_repl_last_uuid(), tombstones);
                    Some(pid)
                }
                Err(e) => {
                    error!("unable to fork a new process because {}", e);
                    return Err(CstError::SystemError);
                }
            }
        };
        Ok((pid, file_name, self.snapshot.0))
    }
}

/**
 *  data management
 */
impl Server {
    pub fn gc(&mut self) {
        match self.replicas.min_uuid() {
            None => return,
            Some(u) => self.db.gc(u),
        }
    }
}

/*
 *  the replicate log system
 *
 */
impl Server {
    pub fn replicate_cmd(&mut self, uuid: u64, cmd_name: &'static str, args: Vec<Message>) {
        //let prev_uuid = self.repl_log.back().map(|(x, _, _)| *x).unwrap_or(1);
        let s: usize = args.iter().map(|x| x.size()).sum();
        self.repl_log.push_back((uuid, cmd_name, args));
        self.repl_log_size += s as u64;
        while self.repl_log_size > self.repl_log_size_limit {
            match self.repl_log.pop_front() {
                None => {
                    error!("the repl_log is empty while repl_log_size is greater than 0");
                }
                Some((u, _, ms)) => {
                    let s: usize = ms.iter().map(|x| x.size()).sum();
                    self.repl_log_size -= s as u64;
                    self.latest_repl_uuid_overflowed = Some(u);
                }
            }
        }
        self.events.trigger(Event::Replicated(uuid));
    }

    pub fn repl_log_next(&self, uuid: u64) -> Option<(u64, Message)> {
        let msg_pos = if uuid == 0 {
            if self.latest_repl_uuid_overflowed.is_some() {
                None
            } else {
                Some(0)
            }
        } else {
            self.repl_log_uuid_index(uuid).map(|x| x+1)
        };
        match msg_pos {
            None => return None,
            Some(pos) => match self.repl_log.get(pos) {
                None => None,
                Some((next_uuid, cmd_name, args)) => {
                    let mut replicates = Vec::with_capacity(args.len() + 5);
                    replicates.push(Message::BulkString("replicate".into()));
                    replicates.push(Message::Integer(self.node_id as i64));
                    replicates.push(Message::Integer(uuid as i64));
                    replicates.push(Message::Integer(*next_uuid as i64));
                    replicates.push(Message::BulkString((*cmd_name).into()));
                    replicates.extend_from_slice(args);
                    Some((*next_uuid, Message::Array(replicates)))
                }
            }
        }
    }

    fn repl_log_uuid_index(&self, uuid: u64) -> Option<usize> {
        let l = self.repl_log.len();
        if l == 0 || self.repl_log[0].0 > uuid {
            return None;
        }
        if uuid > self.repl_log[l-1].0 {
            return None;
        }
        let (mut start, mut end)  = (0usize, l-1);
        loop {
            if end - start == 1 {
                if self.repl_log[start].0 == uuid {
                    return Some(start);
                }
                if self.repl_log[end].0 == uuid {
                    return Some(end);
                }
                return None;
            }
            let middle = (start + end) / 2;
            let uuid_at_middle = self.repl_log[middle].0;
            if uuid_at_middle != uuid && middle == start {
                return None;
            }
            if uuid_at_middle > uuid {
                end = middle;
            } else if uuid_at_middle < uuid {
                start = middle;
            } else {
                return Some(middle)
            }
        }
    }

    pub fn repl_log_at(&self, uuid: u64) -> Option<Message> {
        match self.repl_log_uuid_index(uuid) {
            None => None,
            Some(idx) => match self.repl_log.get(idx) {
                None => None,
                Some((_, cmd_name, args)) => {
                    let mut cmd = Vec::with_capacity(args.len() + 1);
                    cmd.push(Message::BulkString(cmd_name.to_string().into()));
                    cmd.extend_from_slice(args);
                    Some(Message::Array(cmd))
                },
            }
        }
    }

    pub fn repl_log_uuids(&self) -> Vec<u64> {
        self.repl_log.iter().map(|(x, _, _)| *x).collect()
    }

    #[inline]
    pub fn get_repl_first_uuid(&self) -> u64 {
        self.repl_log.front().map(|(u, _, _)| *u).unwrap_or_default()
    }

    #[inline]
    pub fn get_repl_last_uuid(&self) -> u64 {
        self.repl_log.back().map(|(u, _, _)| *u).unwrap_or_default()
    }
}

#[cfg(test)]
mod test {
    use bitflags::_core::time::Duration;
    use tokio::macros::support::thread_rng_n;

    use crate::conf::Config;
    use crate::resp::Message;
    use crate::server::Server;
    static Conf: Config = Config{
        daemon: false,
        node_id: 1,
        node_alias: String::new(),
        ip: String::new(),
        port: 9000,
        addr: String::new(),
        threads: 4,
        log: String::new(),
        work_dir: String::new(),
        tcp_backlog: 1024,
        replica_heartbeat_frequency: 0,
        replica_gossip_frequency: 0
    };

    #[test]
    fn test_replog() {
        let mut server = Server::new(&Conf);
        let random_bytes = |size: usize| -> Vec<u8> {
            (0..size).map(|_|thread_rng_n(26) + 48).map(|x| x as u8).collect()
        };

        // let mut uuids = Vec::with_capacity(100);
        // for _ in 0..100 {
        //     let uuid = server.next_uuid(true);
        //     let args_len = thread_rng_n(5);
        //     let args: Vec<Message> = (0..args_len).map(|_| random_bytes(thread_rng_n(6) as usize)).map(|x| Message::BulkString(x.into())).collect();
        //     uuids.push((uuid, random_bytes(8), args));
        //     std::thread::sleep(Duration::from_millis(1));
        // }
        // for (uuid, cmd, args) in &uuids {
        //     server.replicate_cmd(*uuid, , args.clone());
        // }
        // assert_eq!(server.get_repl_first_uuid(), uuids[0].0);
        // assert_eq!(server.get_repl_last_uuid(), uuids[99].0);
        // for _ in 0..50 {
        //     let idx = thread_rng_n(99) as usize;
        //     let (uuid, cmd, args) = uuids.get(idx).unwrap();
        //     assert_eq!(server.repl_log_uuid_index(*uuid), Some(idx));
        //     assert_eq!(server.get_uuid_after(*uuid), Some(uuids[idx+1].0));
        // }
    }

    #[test]
    fn test_uuid() {
        let mut server = Server::new(&Conf);
        let mut prev = 0;
        for _ in 0..1000 {
            let c = server.next_uuid(true);
            println!("{}, {}", prev, c);
            assert!(c > prev);
            prev = c;
        }
    }
}

// pub struct EventsProducer {
//     replicated: tokio::sync::watch::Sender<u64>,
//     replica_acked: tokio::sync::broadcast::Sender<(u64, u64)>,
//     deletion: tokio::sync::broadcast::Sender<(Bytes, Option<Bytes>, u64)>,
// }
//
// #[derive(Clone, Debug)]
// pub struct EventsConsumer {
//     watching: u8,
//     replicated: tokio::sync::watch::Receiver<u64>,
//     replica_acked: tokio::sync::broadcast::Receiver<(u64, u64)>, // (uuid, uuid_he_acked)
//     deletion: tokio::sync::broadcast::Receiver<(Bytes, Option<Bytes>, u64)>, // (key, field/member, uuid)
// }
//
// pub fn new_events_chann() -> (EventsProducer, EventsConsumer) {
//     let (r_tx, r_rx) = tokio::sync::watch::channel(0);
//     let (ra_tx, ra_rx) = tokio::sync::broadcast::channel(1024);
//     let (d_tx, d_rx) = tokio::sync::broadcast::channel(1024);
//     (EventsProducer{
//         replicated: r_tx,
//         replica_acked: ra_tx,
//         deletion: d_tx
//     }, EventsConsumer{
//         watching: 0,
//         replicated: r_rx,
//         replica_acked: ra_rx,
//         deletion: d_rx
//     })
// }


// TODO, optimization needed
pub struct EventsProducer {
    events: tokio::sync::broadcast::Sender<Event>,
}

#[derive(Debug)]
pub struct EventsConsumer {
    watching: u8,
    events: tokio::sync::broadcast::Receiver<Event>,
}

pub fn new_events_chann() -> (EventsProducer, EventsConsumer) {
    let (tx, rx) = tokio::sync::broadcast::channel(1024);

    (EventsProducer{
        events: tx
    }, EventsConsumer{
        watching: 0,
        events: rx
    })
}

impl EventsProducer {
    fn trigger(&mut self, e: Event) {
        let _ = self.events.send(e);
    }

    pub fn new_consumer(&self) -> EventsConsumer {
        EventsConsumer::new(self.events.subscribe())
    }
}

#[derive(Debug, Clone)]
pub enum Event {
    Replicated(u64),
    ReplicaAcked((u64, u64)),
    Deleted((Bytes, Option<Bytes>, u64)),
}

pub const EVENT_TYPE_REPLICATED: u8 = 1;
pub const EVENT_TYPE_REPLICA_ACKED: u8 = 1<<1;
pub const EVENT_TYPE_DELETED: u8 = 1<<2;

impl EventsConsumer {
    pub fn new(rx: tokio::sync::broadcast::Receiver<Event>) -> Self {
        Self{
            watching: 0,
            events: rx
        }
    }

    pub fn watch(&mut self, t: u8) {
        self.watching |= t;
    }

    pub async fn occured(&mut self) -> Event {
        loop {
            let e = self.events.recv().await.unwrap();
            let flag = match &e {
                Event::Replicated(_) => EVENT_TYPE_REPLICATED,
                Event::ReplicaAcked(_) => EVENT_TYPE_REPLICA_ACKED,
                Event::Deleted(_) => EVENT_TYPE_DELETED,
            };
            if self.watching & flag > 0 {
                return e;
            }
        }
    }
}
