use lazy_static::lazy_static;
use log::{debug, error};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use std::result::Result::Ok;

use nix::unistd::{fork, ForkResult, Pid};
use std::option::Option::Some;
use std::time::Duration;
use tokio::sync::mpsc::Receiver as tokioReceiver;
use tokio::sync::mpsc::Sender as tokioSender;
use tokio::sync::OwnedMutexGuard;
use tokio::task::spawn_local;
use tokio::time::interval_at;
use tokio::time::Instant;

use crate::conf::Config;
use crate::db::DB;
use crate::link::Link;
use crate::object::Object;
use crate::repl_backlog::ReplBacklog;
use crate::replica::replica::{ReplicaIdentity, ReplicaManager};
use crate::snapshot::{SnapshotWriter, SNAPSHOT_FLAG_CHECKSUM};
use crate::stats::Metrics;
use crate::{now_mil, Bytes, CstError};
use once_cell::sync::OnceCell;
use std::fmt::Debug;

lazy_static! {
    pub static ref ACTORS_QUEUE: OnceCell<tokioSender<OwnedMutexGuard<Box<dyn Link + Send>>>> =
        OnceCell::new();
}

pub struct Server {
    pub config: &'static Config,
    pub addr: String,
    pub node_id: u64,
    pub node_alias: String,
    uuid: u64,
    pub expires: HashMap<Bytes, Object>,
    pub db: DB,

    pub replicas: ReplicaManager,
    pub repl_backlog: ReplBacklog,

    // latest time a snapshot was dumped, and the replica ids and their uuids we received at that time
    snapshot: (u64, HashMap<String, u64>),

    latest_dump_time: u64,
    latest_dumped_at_uuid: u64,
    actor_queue: Option<tokioReceiver<OwnedMutexGuard<Box<dyn Link + Send>>>>,
    replicate_acked_events: (EventsProducer<u64>, EventsConsumer<u64>),
    pub metrics: Metrics,
}

impl Server {
    pub fn new(config: &'static Config) -> Self {
        let identity = ReplicaIdentity {
            id: config.node_id,
            addr: config.addr.clone(),
            alias: config.node_alias.clone(),
        };
        let (tx, rx) = tokio::sync::mpsc::channel(102400);
        let _ = ACTORS_QUEUE.set(tx);

        Server {
            node_id: config.node_id,
            node_alias: config.node_alias.clone(),
            addr: config.addr.clone(),
            config,
            uuid: 1,
            expires: HashMap::new(),
            db: DB::empty(),

            repl_backlog: ReplBacklog::new(config.node_id, config.repl_backlog_limit),
            replicas: ReplicaManager::new(identity),
            snapshot: (0, Default::default()),
            latest_dump_time: 0,
            latest_dumped_at_uuid: 0,
            actor_queue: Some(rx),
            replicate_acked_events: new_events_chann(),
            metrics: Default::default(),
        }
    }

    pub fn new_replica_acked_event_watcher(&self) -> EventsConsumer<u64> {
        self.replicate_acked_events.0.new_consumer()
    }

    pub async fn run(server: Rc<RefCell<Self>>) -> Result<(), std::io::Error> {
        let server_cc = server.clone();
        spawn_local(async move {
            Self::cron(server_cc).await;
        });
        let mut actor_queue = {
            let mut s = server.borrow_mut();
            s.actor_queue.take().unwrap()
        };
        while let Some(mut l) = actor_queue.recv().await {
            l.serve(&mut *(*server).borrow_mut());
        }
        Ok(())
    }

    async fn cron(server: Rc<RefCell<Server>>) {
        let hz = server.borrow().config.hz;
        let mut timer = interval_at(
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(hz as u64),
        );
        loop {
            {
                let mut s = server.deref().borrow_mut();
                let _ = s.next_uuid(now_mil(), true);
            }
            let _ = timer.tick().await;
            server.deref().borrow_mut().gc();
            // check for new replicas
            let _ = server.clone();
        }
    }

    // generate a uuid that is associated with the command currently being executed
    // this uuid is also used as a timestamp.
    // for writing,  we always return a bigger uuid.
    pub fn next_uuid(&mut self, now: u64, is_write: bool) -> u64 {
        let (time_mil, mut sequnce) = (self.uuid >> 22, (self.uuid & ((1 << 22) - 1)));
        // let now = now_mil();
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
        debug!(
            "begin to dump, current_dir is {:?}",
            std::env::current_dir()
        );
        let tmp_name = format!("snapshot_{}", chrono::Local::now().timestamp());
        let f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(tmp_name.clone())?;
        debug!("tmp file {} created", tmp_name);

        let mut w = SnapshotWriter::new(4096, f);
        w.write_bytes(b"CONSTDB")?;
        w.write_bytes([0u8, 1, 1, 1].as_ref())?;

        // dump my metadatas
        let _ = w
            .write_integer(self.node_id as i64)?
            .write_integer(self.node_alias.len() as i64)?
            .write_bytes(self.node_alias.as_bytes())?
            .write_integer(self.addr.len() as i64)?
            .write_bytes(self.addr.as_ref())?
            .write_integer(self.repl_backlog.last_uuid() as i64)?;

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
        let pid = if self.snapshot.0 > self.repl_backlog.first_uuid() {
            // Congratulations! we've dumped a snapshot not long before, so we can use that snapshot
            debug!("we've dumped a snapshot not long before, we can use that one!");
            None
        } else {
            // we need to dump a fresh snapshot now!
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
                    tombstones.insert(self.addr.clone(), self.repl_backlog.last_uuid());
                    self.snapshot = (self.repl_backlog.last_uuid(), tombstones);
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

#[cfg(test)]
mod test {
    use std::time::Duration;
    use tokio::macros::support::thread_rng_n;

    use crate::conf::Config;
    use crate::now_mil;
    use crate::resp::Message;
    use crate::server::Server;

    static Conf: Config = Config {
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
        repl_backlog_limit: 0,
        replica_heartbeat_frequency: 0,
        replica_gossip_frequency: 0,
        log_level: "".to_string(),
        max_exec_per_round: 0,
        hz: 0,
    };

    #[test]
    fn test_uuid() {
        let mut server = Server::new(&Conf);
        let mut prev = 0;
        for _ in 0..1000 {
            let c = server.next_uuid(now_mil(), true);
            println!("{}, {}", prev, c);
            assert!(c > prev);
            prev = c;
        }
    }
}

pub trait Event: Clone + Debug + Default {
    fn happened_after(&self, other: &Self) -> bool;
}

impl Event for u64 {
    fn happened_after(&self, other: &Self) -> bool {
        self > other
    }
}

#[derive(Debug)]
pub struct EventsProducer<T: Event> {
    events: tokio::sync::watch::Sender<T>,
}

#[derive(Debug)]
pub struct EventsConsumer<T: Event> {
    events: tokio::sync::watch::Receiver<T>,
}

pub fn new_events_chann<T: Event>() -> (EventsProducer<T>, EventsConsumer<T>) {
    let (tx, rx) = tokio::sync::watch::channel(T::default());
    (EventsProducer { events: tx }, EventsConsumer { events: rx })
}

impl<T: Event> EventsProducer<T> {
    pub fn trigger(&self, e: T) {
        let _ = self.events.send(e);
    }

    pub fn new_consumer(&self) -> EventsConsumer<T> {
        EventsConsumer::new(self.events.subscribe())
    }
}

impl<T: Event> EventsConsumer<T> {
    pub fn new(rx: tokio::sync::watch::Receiver<T>) -> Self {
        Self { events: rx }
    }

    pub async fn occured(&mut self, current: &T) {
        if self.events.borrow().happened_after(&current) {
            return;
        }
        loop {
            if self.events.borrow().happened_after(&current) {
                return;
            }
            self.events.changed().await.unwrap();
        }
    }
}
