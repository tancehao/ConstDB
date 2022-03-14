use spin::RwLock;
use crate::link::Client;
use crate::resp::Message;
use crate::CstError;
use crate::cmd::NextArg;
use failure::_core::fmt::{Display, Formatter};
use crate::server::Server;
use std::sync::atomic::{AtomicU64};
use failure::_core::sync::atomic::{Ordering, AtomicUsize};
use crate::conf::{GLOBAL_CONF, CONF_PATH};
use sysinfo::{SystemExt, ProcessExt};

lazy_static!{
    static ref GLOBAL_METRICS: RwLock<Metrics> = RwLock::new(Metrics::default());
    pub static ref CURRENT_MEMORY: AtomicUsize = AtomicUsize::new(0);
    pub static ref CURRENT_CLIENTS: AtomicU64 = AtomicU64::new(0);
    pub static ref TOTAL_NETWORK_INPUT_BYTES: AtomicU64 = AtomicU64::new(0);
    pub static ref TOTAL_NETWORK_OUTPUT_BYTES: AtomicU64 = AtomicU64::new(0);
    pub static ref TOTAL_CONNS_RECEIVED: AtomicU64 = AtomicU64::new(0);
}

#[derive(Debug, Default, Clone)]
pub struct Metrics {
    server: ServerBasic,
    clients: Clients,
    memory: Memory,
    stats: Stats,
    replication: Replication,
    cpu: CPU,
    keyspace: Keyspace,
}

impl Metrics {
    pub fn incr_cmd_processed(&mut self) {
        self.stats.total_commands_processed += 1;
    }

    pub fn add_connections_received(&mut self) {
        self.stats.total_connections_received += 1;
    }

    pub fn add_keyspace_keys(&mut self) {
        self.keyspace.expires += 1;
    }

    pub fn add_keyspace_expires(&mut self) {
        self.keyspace.expires += 1;
    }
}

impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.server.fmt(f)?;
        f.write_str("\n")?;
        self.clients.fmt(f)?;
        f.write_str("\n")?;
        self.memory.fmt(f)?;
        f.write_str("\n")?;
        self.stats.fmt(f)?;
        f.write_str("\n")?;
        self.replication.fmt(f)?;
        f.write_str("\n")?;
        self.cpu.fmt(f)?;
        f.write_str("\n")?;
        self.keyspace.fmt(f)
    }
}

fn refresh_metrics(server: &mut Server) {
    let mem = CURRENT_MEMORY.load(Ordering::Relaxed);
    let clients = CURRENT_CLIENTS.load(Ordering::Relaxed);
    let net_input = TOTAL_NETWORK_INPUT_BYTES.load(Ordering::Relaxed);
    let net_output = TOTAL_NETWORK_OUTPUT_BYTES.load(Ordering::Relaxed);
    let conns_rcvd = TOTAL_CONNS_RECEIVED.load(Ordering::Relaxed);
    let mut g = GLOBAL_METRICS.write();
    g.memory.memory_change(mem);
    g.stats.total_net_input_bytes = net_input;
    g.stats.total_net_output_bytes = net_output;
    g.clients.connected_clients = clients;
    g.stats.total_connections_received += server.metrics.stats.total_connections_received;
    server.metrics.stats.total_connections_received = 0;
    g.stats.total_commands_processed += server.metrics.stats.total_commands_processed;
    server.metrics.stats.total_commands_processed = 0;
    g.stats.total_connections_received = conns_rcvd;
}

#[derive(Clone, Debug)]
struct ServerBasic {
    version: String,
    executable: String,
    config_file: String,
    uptime_in_seconds: u64,
    tcp_port: u16,
    os: String,
}

impl Default for ServerBasic {
    fn default() -> Self {
        let conf = &GLOBAL_CONF;
        let s = sysinfo::System::new();
        let mut server = ServerBasic{
            version: env!("CARGO_PKG_VERSION").to_string(),
            executable: "".to_string(),
            config_file: CONF_PATH.clone(),
            uptime_in_seconds: s.uptime(),
            tcp_port: conf.port,
            os: s.os_version().unwrap_or_default()
        };
        // TODO
        if let Ok(Some(proc)) = sysinfo::get_current_pid().map(|x| s.process(x)) {
            server.executable = proc.exe().to_str().map(|x| x.to_string()).unwrap_or_default();
            server.uptime_in_seconds = proc.run_time();
        }
        server
    }
}

impl Display for ServerBasic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("# Server\n")?;
        f.write_fmt(format_args!("ConstDB_version:{}\n", self.version))?;
        f.write_fmt(format_args!("executable:{}\n", self.executable))?;
        f.write_fmt(format_args!("config_file:{}\n", self.config_file))?;
        f.write_fmt(format_args!("uptime_in_seconds:{}\n", self.uptime_in_seconds))?;
        f.write_fmt(format_args!("os:{}\n", self.os))?;
        f.write_fmt(format_args!("tcp_port:{}\n", self.tcp_port))
    }
}

#[derive(Default, Debug, Clone)]
struct Clients {
    connected_clients: u64,
    client_recent_max_input_buffer: usize,
    client_recent_max_output_buffer: usize,
    blocked_clients: u64,
}

impl Display for Clients {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("# Clients\n")?;
        f.write_fmt(format_args!("connected_clients:{}\n", self.connected_clients))?;
        f.write_fmt(format_args!("client_recent_max_input_buffer:{}\n", self.client_recent_max_input_buffer))?;
        f.write_fmt(format_args!("client_recent_max_output_buffer:{}\n", self.client_recent_max_output_buffer))?;
        f.write_fmt(format_args!("blocked_clients:{}\n", self.blocked_clients))

    }
}

#[derive(Default, Debug, Clone)]
struct Memory {
    used_memory: usize,
    used_memory_rss: usize,
    used_memory_peak: usize,
    used_memory_dataset: usize,
    maxmemory: usize,
}

impl Memory {
    #[inline]
    fn memory_change(&mut self, m: usize) {
        self.used_memory = m;
        if self.used_memory_peak < m {
            self.used_memory_peak = m;
        }
    }
}

impl Display for Memory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("# Memory\n")?;
        f.write_fmt(format_args!("used_memory:{}\n", self.used_memory))?;
        f.write_fmt(format_args!("used_memory_rss:{}\n", self.used_memory_rss))?;
        f.write_fmt(format_args!("used_memory_peak:{}\n", self.used_memory_peak))?;
        f.write_fmt(format_args!("used_memory_dataset:{}\n", self.used_memory_dataset))?;
        f.write_fmt(format_args!("maxmemory:{}\n", self.maxmemory))
    }
}

#[derive(Debug, Default, Clone)]
pub struct Stats {
    total_connections_received: u64,
    total_commands_processed: u64,
    instantaneous_ops_per_sec: u64,
    total_net_input_bytes: u64,
    total_net_output_bytes: u64,
    expired_keys: u64,
}

impl Display for Stats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("# Stats\n")?;
        f.write_fmt(format_args!("total_connections_received:{}\n", self.total_connections_received))?;
        f.write_fmt(format_args!("total_commands_processed:{}\n", self.total_commands_processed))?;
        f.write_fmt(format_args!("instantaneous_ops_per_sec:{}\n", self.instantaneous_ops_per_sec))?;
        f.write_fmt(format_args!("total_net_input_bytes:{}\n", self.total_net_input_bytes))?;
        f.write_fmt(format_args!("total_net_output_bytes:{}\n", self.total_net_output_bytes))?;
        f.write_fmt(format_args!("expired_keys:{}\n", self.expired_keys))
    }
}

#[derive(Debug, Default, Clone)]
pub struct Replication {
    connected_replicas: u32,
    repl_backlog_active: bool,
    repl_backlog_size: usize,
    repl_backlog_first_uuid: u64,
    repl_backlog_hislen: usize,
}

impl Display for Replication {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("# Replication\n")?;
        f.write_fmt(format_args!("connected_replicas:{}\n", self.connected_replicas))?;
        f.write_fmt(format_args!("repl_backlog_active:{}\n", self.repl_backlog_active))?;
        f.write_fmt(format_args!("repl_backlog_size:{}\n", self.repl_backlog_size))?;
        f.write_fmt(format_args!("repl_backlog_first_uuid:{}\n", self.repl_backlog_first_uuid))?;
        f.write_fmt(format_args!("repl_backlog_hislen:{}\n", self.repl_backlog_hislen))
    }
}

#[derive(Default, Debug, Clone)]
pub struct CPU {
    used_cpu_sys: f64,
    used_cpu_user: f64,
    used_cpu_sys_children: f64,
    used_cpu_user_children: f64,
}

impl Display for CPU {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("# CPU\n")?;
        f.write_fmt(format_args!("used_cpu_sys:{}\n", self.used_cpu_sys))?;
        f.write_fmt(format_args!("used_cpu_user:{}\n", self.used_cpu_user))?;
        f.write_fmt(format_args!("used_cpu_sys_children:{}\n", self.used_cpu_sys_children))?;
        f.write_fmt(format_args!("used_cpu_user_children:{}\n", self.used_cpu_user_children))
    }
}

#[derive(Default, Debug, Clone)]
pub struct Keyspace {
    keys: u64,
    expires: u64,
    agv_ttl: u64,
}

impl Display for Keyspace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("# Keyspace\n")?;
        f.write_fmt(format_args!("db0:keys={},expires={},evg_ttl={}\n", self.keys, self.expires, self.agv_ttl))
    }
}

#[inline]
pub fn mem_allocated(size: usize) {
    CURRENT_MEMORY.fetch_add(size, Ordering::Relaxed);
}

pub fn mem_released(size: usize) {
    CURRENT_MEMORY.fetch_sub(size, Ordering::Relaxed);
}

#[inline]
pub fn incr_clients() {
    CURRENT_CLIENTS.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn incr_conns_received() {
    TOTAL_CONNS_RECEIVED.fetch_add(1, Ordering::Relaxed);
}

#[inline]
pub fn decr_clients() {
    CURRENT_CLIENTS.fetch_sub(1, Ordering::Relaxed);
}

#[inline]
pub fn add_network_input_bytes(size: u64) {
    TOTAL_NETWORK_INPUT_BYTES.fetch_add(size, Ordering::Relaxed);
}

#[inline]
pub fn add_network_output_bytes(size: u64) {
    TOTAL_NETWORK_OUTPUT_BYTES.fetch_add(size, Ordering::Relaxed);
}

pub fn info_command(server: &mut Server, _client: Option<&mut Client>, _nodeid: u64, _uuid: u64, args: Vec<Message>) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    refresh_metrics(server);
    let m = GLOBAL_METRICS.read();
    let resp = match args.next_string() {
        Ok(part) => match part.to_ascii_lowercase().as_str() {
            "server" => format!("{}", m.server),
            "memory" => format!("{}", m.memory),
            "clients" => format!("{}", m.clients),
            "stats" => format!("{}", m.stats),
            "replication" => format!("{}", m.replication),
            "cpu" => format!("{}", m.cpu),
            "keyspace" => format!("{}", m.keyspace),
            o => return Err(CstError::UnknownSubCmd("info".to_string(), o.to_string()))
        }
        Err(_) => format!("{}", m),
    };
    Ok(Message::BulkString(resp.into()))
}