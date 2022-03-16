#[macro_use]
extern crate async_trait;
extern crate bitflags;
#[macro_use]
extern crate clap;
pub extern crate colour;
#[macro_use]
extern crate failure;
#[macro_use]
pub extern crate  lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

use std::fs::OpenOptions;
use std::io::Error;
use std::io::Write;
use std::ops::Deref;
use std::sync::Arc;

use bitflags::_core::hash::{Hash, Hasher};
use log4rs::append::console::ConsoleAppender;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::policy::compound::roll::delete::DeleteRoller;
use log4rs::append::rolling_file::policy::compound::trigger::size::SizeTrigger;
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::Config;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use log::LevelFilter;
use nix::unistd::{fork, ForkResult};

use crate::conf::GLOBAL_CONF;
use crate::server::Server;
use failure::_core::alloc::{GlobalAlloc, Layout};
use crate::stats::{mem_allocated, mem_released};

#[macro_use]
pub mod resp;
#[macro_use]
pub mod link;

pub mod server;
pub mod object;
pub mod cmd;
pub mod conf;
pub mod conn;
pub mod snapshot;
pub mod db;
pub mod type_set;
pub mod type_hash;
pub mod type_counter;
pub mod replica;
pub mod stats;
pub mod crdt;

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
            println!("unable to set current dir to {} because {}", config.work_dir, e);
            return;
        }
    }
    println!("daemon: {}", config.daemon);
    if config.daemon {
        match unsafe{fork()} {
            Err(e) => {
                error!("unable to fork a child process because {}", e);
                return;
            }
            Ok(ForkResult::Parent {child: pid}) => {
                match OpenOptions::new().create(true).write(true).truncate(true).open("pid") {
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
            },
            Ok(ForkResult::Child) => {}
        }
    }
    let pe = Box::new(PatternEncoder::new("{d} {l} {f}:{L} - {m}{n}"));
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
            .build(Root::builder().appender("log").build(LevelFilter::Debug))
            .unwrap();
        let _handle = log4rs::init_config(config).unwrap();
    } else {
        let config = Config::builder()
            .appender(Appender::builder().build(
                "stdout",
                Box::new(ConsoleAppender::builder().encoder(pe).build()),
            ))
            .build(Root::builder().appender("stdout").build(LevelFilter::Debug))
            .unwrap();
        let _handle = log4rs::init_config(config).unwrap();
    }

    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(GLOBAL_CONF.threads).enable_all().build().unwrap();
    let ls = tokio::task::LocalSet::new();
    ls.block_on(&rt, async move {
        Server::run(config).await.unwrap();
    });
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
    #[fail(display = "invalid checksum of snapshot")]
    InvalidSnapshotChecksum,
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
            Err(_) => format!("0X{}", base16::encode_lower(&bs))
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