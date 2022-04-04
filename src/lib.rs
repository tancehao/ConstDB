#[macro_use]
extern crate async_trait;
extern crate bitflags;
#[macro_use]
extern crate clap;
pub extern crate colour;
#[macro_use]
extern crate failure;
#[macro_use]
pub extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
extern crate core_affinity;
extern crate serde;
extern crate tokio;

use std::fs::OpenOptions;
use std::io::Error;
use std::io::Write;
use std::ops::Deref;
use std::sync::Arc;

use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::rolling_file::policy::compound::roll::delete::DeleteRoller;
use log4rs::append::rolling_file::policy::compound::trigger::size::SizeTrigger;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use log4rs::Config;
use nix::unistd::{fork, ForkResult};
use std::hash::{Hash, Hasher};

use crate::conf::GLOBAL_CONF;
use crate::client::Client;
use crate::link::SharedLink;
use crate::server::Server;
use crate::stats::{mem_allocated, mem_released, start_local_metric_collector, incr_clients};
use std::alloc::{GlobalAlloc, Layout};
use std::cell::RefCell;
use std::rc::Rc;
use tokio::sync::mpsc::Sender;
use crate::lib::utils::run_async_in_current_thread;
use std::net::SocketAddr;
use tokio::net::TcpSocket;

#[macro_use]
pub mod resp;
#[macro_use]
pub mod link;

mod cmd;
mod conf;
pub mod conn;
mod crdt;
mod db;
mod object;
mod repl_backlog;
mod replica;
mod server;
mod snapshot;
mod stats;
mod type_counter;
mod type_hash;
mod type_list;
mod type_set;
mod client;
mod type_register;

pub mod lib {
    pub mod utils;
}

#[global_allocator]
static ALLOCATOR: CAlloc = CAlloc(jemallocator::Jemalloc);

struct CAlloc(jemallocator::Jemalloc);

unsafe impl GlobalAlloc for CAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        mem_allocated(layout.size());
        self.0.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        mem_released(layout.size());
        self.0.dealloc(ptr, layout)
    }
}

pub fn run_server() {
    let config = &GLOBAL_CONF;
    if config.work_dir != "" {
        if let Err(e) = std::env::set_current_dir(&config.work_dir) {
            println!(
                "unable to set current dir to {} because {}",
                config.work_dir, e
            );
            return;
        }
    }
    if config.daemon {
        match unsafe { fork() } {
            Err(e) => {
                error!("unable to fork a child process because {}", e);
                return;
            }
            Ok(ForkResult::Parent { child: pid }) => {
                match OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open("pid")
                {
                    Ok(mut f) => {
                        if let Err(_) = f.write(format!("{}", pid).as_bytes()) {
                            error!("unable to write pid file");
                        }
                        return;
                    }
                    Err(e) => {
                        error!("unable to create pid file because {}", e);
                        return;
                    }
                }
            }
            Ok(ForkResult::Child) => {}
        }
    }
    let pe = Box::new(PatternEncoder::new("{d} {l} {f}:{L} - {m}{n}"));
    let log_level = match config.log_level.as_str() {
        "DEBUG" => LevelFilter::Debug,
        "INFO" => LevelFilter::Info,
        "WARN" => LevelFilter::Warn,
        "TRACE" => LevelFilter::Trace,
        "OFF" => LevelFilter::Off,
        "ERROR" => LevelFilter::Error,
        _ => unreachable!(),
    };
    if config.log != "" {
        let policy = CompoundPolicy::new(
            Box::new(SizeTrigger::new(10737418240)),
            Box::new(DeleteRoller::new()),
        );
        let logfile = RollingFileAppender::builder()
            .encoder(pe)
            .build(config.log.clone(), Box::new(policy))
            .unwrap();
        let config = Config::builder()
            .appender(Appender::builder().build("log", Box::new(logfile)))
            .build(Root::builder().appender("log").build(log_level))
            .unwrap();
        let _handle = log4rs::init_config(config).unwrap();
    } else {
        let config = Config::builder()
            .appender(Appender::builder().build(
                "stdout",
                Box::new(ConsoleAppender::builder().encoder(pe).build()),
            ))
            .build(Root::builder().appender("stdout").build(log_level))
            .unwrap();
        let _handle = log4rs::init_config(config).unwrap();
    }
    let mut handles = vec![];
    let mut client_chans: Vec<Sender<SharedLink>> = vec![];
    (0..GLOBAL_CONF.threads).for_each(|i| {
        let (client_tx, mut client_rx) = tokio::sync::mpsc::channel(1024);
        client_chans.push(client_tx);
        let handle = std::thread::Builder::new()
            .name(format!("constdb-io-worker-{}", i))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let ls = tokio::task::LocalSet::new();
                ls.block_on(&rt, async move {
                    start_local_metric_collector();
                    while let Some(mut client) = client_rx.recv().await {
                        tokio::task::spawn_local(async move {
                            client.prepare().await;
                        });
                    }
                });
            })
            .unwrap();
        handles.push(handle);
    });

    let addr = config.addr.clone();
    let tcp_backlog = config.tcp_backlog;
    let handle = std::thread::Builder::new()
        .name("tcp-listener".to_string())
        .spawn(move || {
            run_async_in_current_thread(async move {
                let addr = addr.parse::<SocketAddr>().unwrap();
                let socket = TcpSocket::new_v4().unwrap();
                if socket.set_reuseaddr(true).is_err() || socket.set_reuseport(true).is_err() {
                    error!("Unable to reuse addr or port of address {}", addr);
                    std::process::exit(-1);
                }
                if let Err(e) = socket.bind(addr) {
                    error!("Unable to bind to address for {:?}", e);
                    std::process::exit(-1);
                }
                let listener = match socket.listen(tcp_backlog) {
                    Err(e) => {
                        error!("Unable to listen to address for {:?}", e);
                        std::process::exit(-1);
                    }
                    Ok(l) => l,
                };
                let mut i = 0;
                loop {
                    match listener.accept().await {
                        Err(e) => {
                            error!("Failed to accept new connection because {}", e);
                            std::process::exit(-1);
                        }
                        Ok((conn, peer_addr)) => {
                            let _ = conn.set_nodelay(true);
                            incr_clients();
                            let sc = SharedLink::from(Client::new(conn, peer_addr.to_string()));
                            i += 1;
                            loop {
                                if client_chans[i % client_chans.len()]
                                    .try_send(sc.clone())
                                    .is_ok()
                                {
                                    break;
                                }
                                i += 1;
                            }
                        }
                    }
                }
            });
        }).unwrap();
    handles.push(handle);

    let handle = std::thread::Builder::new()
        .name("etc".to_string())
        .spawn(move || {
            run_async_in_current_thread(async move {
                crate::stats::pprof().await;
            });
        }).unwrap();
    handles.push(handle);

    let handle = std::thread::Builder::new()
        .name("constdb-main".to_string())
        .spawn(move || {
            match core_affinity::get_core_ids() {
                Some(core_ids) => {
                    if !core_ids.is_empty() {
                        core_affinity::set_for_current(core_ids[0]);
                    }
                }
                None => {}
            }

            let server = Rc::new(RefCell::new(Server::new(config)));
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let ls = tokio::task::LocalSet::new();
            ls.block_on(&rt, async move {
                start_local_metric_collector();
                Server::run(server).await.unwrap();
            });
        })
        .unwrap();
    handles.push(handle);
    for h in handles {
        h.join().unwrap();
    }
}

#[derive(Fail, Debug)]
pub enum CstError {
    #[fail(display = "system error")]
    SystemError,
    #[fail(display = "ERR wrong number of arguments for this command")]
    WrongArity,
    #[fail(display = "incompatible command against the type")]
    InvalidType,
    #[fail(display = "need more message")]
    NeedMoreMsg,
    #[fail(display = "unknown message format {}", _0)]
    InvalidRequestMsg(String),
    #[fail(display = "invalid data in snapshot at offset {}", _0)]
    InvalidSnapshot(usize),
    #[fail(display = "the connection with {} is broken", _0)]
    ConnBroken(String),
    #[fail(display = "io error {}", _0)]
    IoError(Error),
    #[fail(display = "unknown command {}", _0)]
    UnknownCmd(String),
    #[fail(display = "unknown subcommand {} following {}", _0, _1)]
    UnknownSubCmd(String, String),
    #[fail(display = "the replica is delayed")]
    ReplicateDelayed,
    #[fail(display = "some commands from the replica {} are missed", _0)]
    ReplicateCommandsLost(String),
    #[fail(display = "replica nodeid already exist")]
    ReplicaNodeAlreadyExist,
}

impl From<Error> for CstError {
    fn from(e: Error) -> Self {
        Self::IoError(e)
    }
}

#[derive(Clone, Debug)]
pub struct Bytes(Arc<Vec<u8>>);

impl Bytes {
    pub fn to_string(&self) -> String {
        String::from(self.clone())
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(v: Vec<u8>) -> Self {
        Self(Arc::new(v))
    }
}

impl From<&[u8]> for Bytes {
    fn from(v: &[u8]) -> Self {
        v.to_vec().into()
    }
}

impl From<&str> for Bytes {
    fn from(s: &str) -> Self {
        s.as_bytes().into()
    }
}

impl From<String> for Bytes {
    fn from(s: String) -> Self {
        s.as_bytes().into()
    }
}

impl From<CstError> for Bytes {
    fn from(e: CstError) -> Self {
        format!("{}", e).into()
    }
}

impl Hash for Bytes {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl PartialEq for Bytes {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for Bytes {}

impl From<Bytes> for String {
    fn from(b: Bytes) -> Self {
        let bs = b.0.deref().clone();
        match String::from_utf8(bs.clone()) {
            Ok(s) => s,
            Err(_) => format!("0X{}", base16::encode_lower(&bs)),
        }
    }
}

impl Bytes {
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    #[inline]
    pub fn clone_inner(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

#[inline]
fn now_secs() -> u64 {
    chrono::Utc::now().timestamp() as u64
}

#[inline]
fn now_mil() -> u64 {
    chrono::Utc::now().timestamp_millis() as u64
}
