use crate::cmd::NextArg;
use crate::conf::{CONF_PATH, GLOBAL_CONF};
use crate::link::{Client, Link, LinkType, SharedLink};
use crate::resp::Message;
use crate::server::Server;
use crate::CstError;
use spin::RwLock;
use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use sysinfo::{ProcessExt, SystemExt};
use tokio::time::sleep;

lazy_static! {
    static ref GLOBAL_METRICS: RwLock<Metrics> = RwLock::new(Metrics::default());
    pub static ref TOTAL_CONNS_RECEIVED: AtomicU64 = AtomicU64::new(0);
}

thread_local! {
    pub static LOCAL_STATS: RefCell<Metrics> = RefCell::new(Metrics::default());
    pub static LOCAL_MEMORY: RefCell<usize> = RefCell::new(0);
    pub static LOCAL_INPUT_BYTES: RefCell<u64> = RefCell::new(0);
    pub static LOCAL_OUTPUT_BYTES: RefCell<u64> = RefCell::new(0);
    pub static LOCAL_CLIENTS: RefCell<u32> = RefCell::new(0);
    static COLLECTOR_STOPPED: RefCell<bool> = RefCell::new(false);
}

#[derive(Debug)]
pub struct StatsCollector {
    addr: String,
    memory: (usize, usize),
    input_bytes: (u64, u64),
    output_bytes: (u64, u64),
    clients: (u32, u32),
}

#[async_trait]
impl Link for StatsCollector {
    fn addr(&self) -> &str {
        self.addr.as_str()
    }

    fn link_type(&self) -> LinkType {
        LinkType::StatsCollector
    }

    fn serve(&mut self, server: &mut Server) {
        let metrics = &mut server.metrics;
        metrics.clients.connected_clients = metrics
            .clients
            .connected_clients
            .wrapping_add(self.clients.0);
        metrics.clients.connected_clients = metrics
            .clients
            .connected_clients
            .wrapping_sub(self.clients.1);
        self.clients = (0, self.clients.0);

        metrics.memory.used_memory = metrics.memory.used_memory.wrapping_add(self.memory.0);
        metrics.memory.used_memory = metrics.memory.used_memory.wrapping_sub(self.memory.1);
        self.memory = (0, self.memory.0);

        metrics.stats.total_net_input_bytes = metrics
            .stats
            .total_net_input_bytes
            .wrapping_add(self.input_bytes.0);
        metrics.stats.total_net_input_bytes = metrics
            .stats
            .total_net_input_bytes
            .wrapping_sub(self.input_bytes.1);
        self.input_bytes = (0, self.input_bytes.0);

        metrics.stats.total_net_output_bytes += self.output_bytes.0;
        metrics.stats.total_net_output_bytes -= self.output_bytes.1;
        metrics.stats.total_net_output_bytes = metrics
            .stats
            .total_net_output_bytes
            .wrapping_add(self.output_bytes.0);
        metrics.stats.total_net_output_bytes = metrics
            .stats
            .total_net_output_bytes
            .wrapping_sub(self.output_bytes.1);
    }

    async fn prepare(&mut self) {
        sleep(Duration::from_secs(1)).await;
        self.clients.0 = LOCAL_CLIENTS.with(|x| *x.borrow());
        self.memory.0 = LOCAL_MEMORY.with(|x| *x.borrow());
        self.input_bytes.0 = LOCAL_INPUT_BYTES.with(|x| *x.borrow());
        self.output_bytes.0 = LOCAL_OUTPUT_BYTES.with(|x| *x.borrow());
    }

    fn to_serve(&self) -> bool {
        true
    }

    fn to_close(&self) -> bool {
        COLLECTOR_STOPPED.with(|x| *x.borrow())
    }

    async fn close(&mut self) -> Result<(), CstError> {
        Ok(())
    }
}

pub fn start_local_metric_collector() {
    let c = StatsCollector {
        addr: std::thread::current()
            .name()
            .unwrap_or_default()
            .to_string(),
        memory: (0, 0),
        input_bytes: (0, 0),
        output_bytes: (0, 0),
        clients: (0, 0),
    };
    let mut sc = SharedLink::from(c);
    tokio::task::spawn_local(async move {
        sleep(Duration::from_secs(3)).await;
        sc.prepare().await;
    });
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
        let mut server = ServerBasic {
            version: env!("CARGO_PKG_VERSION").to_string(),
            executable: "".to_string(),
            config_file: CONF_PATH.clone(),
            uptime_in_seconds: s.uptime(),
            tcp_port: conf.port,
            os: s.os_version().unwrap_or_default(),
        };
        // TODO
        if let Ok(Some(proc)) = sysinfo::get_current_pid().map(|x| s.process(x)) {
            server.executable = proc
                .exe()
                .to_str()
                .map(|x| x.to_string())
                .unwrap_or_default();
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
        f.write_fmt(format_args!(
            "uptime_in_seconds:{}\n",
            self.uptime_in_seconds
        ))?;
        f.write_fmt(format_args!("os:{}\n", self.os))?;
        f.write_fmt(format_args!("tcp_port:{}\n", self.tcp_port))
    }
}

#[derive(Default, Debug, Clone)]
struct Clients {
    connected_clients: u32,
    client_recent_max_input_buffer: usize,
    client_recent_max_output_buffer: usize,
    blocked_clients: u32,
}

impl Display for Clients {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("# Clients\n")?;
        f.write_fmt(format_args!(
            "connected_clients:{}\n",
            self.connected_clients
        ))?;
        f.write_fmt(format_args!(
            "client_recent_max_input_buffer:{}\n",
            self.client_recent_max_input_buffer
        ))?;
        f.write_fmt(format_args!(
            "client_recent_max_output_buffer:{}\n",
            self.client_recent_max_output_buffer
        ))?;
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

impl Display for Memory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("# Memory\n")?;
        f.write_fmt(format_args!("used_memory:{}\n", self.used_memory))?;
        f.write_fmt(format_args!("used_memory_rss:{}\n", self.used_memory_rss))?;
        f.write_fmt(format_args!("used_memory_peak:{}\n", self.used_memory_peak))?;
        f.write_fmt(format_args!(
            "used_memory_dataset:{}\n",
            self.used_memory_dataset
        ))?;
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
        f.write_fmt(format_args!(
            "total_connections_received:{}\n",
            self.total_connections_received
        ))?;
        f.write_fmt(format_args!(
            "total_commands_processed:{}\n",
            self.total_commands_processed
        ))?;
        f.write_fmt(format_args!(
            "instantaneous_ops_per_sec:{}\n",
            self.instantaneous_ops_per_sec
        ))?;
        f.write_fmt(format_args!(
            "total_net_input_bytes:{}\n",
            self.total_net_input_bytes
        ))?;
        f.write_fmt(format_args!(
            "total_net_output_bytes:{}\n",
            self.total_net_output_bytes
        ))?;
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
        f.write_fmt(format_args!(
            "connected_replicas:{}\n",
            self.connected_replicas
        ))?;
        f.write_fmt(format_args!(
            "repl_backlog_active:{}\n",
            self.repl_backlog_active
        ))?;
        f.write_fmt(format_args!(
            "repl_backlog_size:{}\n",
            self.repl_backlog_size
        ))?;
        f.write_fmt(format_args!(
            "repl_backlog_first_uuid:{}\n",
            self.repl_backlog_first_uuid
        ))?;
        f.write_fmt(format_args!(
            "repl_backlog_hislen:{}\n",
            self.repl_backlog_hislen
        ))
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
        f.write_fmt(format_args!(
            "used_cpu_sys_children:{}\n",
            self.used_cpu_sys_children
        ))?;
        f.write_fmt(format_args!(
            "used_cpu_user_children:{}\n",
            self.used_cpu_user_children
        ))
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
        f.write_fmt(format_args!(
            "db0:keys={},expires={},evg_ttl={}\n",
            self.keys, self.expires, self.agv_ttl
        ))
    }
}

#[inline]
pub fn mem_allocated(size: usize) {
    LOCAL_MEMORY.with(|x| {
        let mut xx = x.borrow_mut();
        *xx = xx.wrapping_add(size);
    });
}

#[inline]
pub fn mem_released(size: usize) {
    LOCAL_MEMORY.with(|x| {
        let mut xx = x.borrow_mut();
        *xx = xx.wrapping_sub(size);
    });
}

#[inline]
pub fn incr_clients() {
    LOCAL_CLIENTS.with(|x| {
        let mut xx = x.borrow_mut();
        *xx = xx.wrapping_add(1);
    });
}

#[inline]
pub fn decr_clients() {
    LOCAL_CLIENTS.with(|x| {
        let mut xx = x.borrow_mut();
        *xx = xx.wrapping_sub(1)
    });
}

#[inline]
pub fn add_network_input_bytes(size: u64) {
    LOCAL_INPUT_BYTES.with(|x| {
        let mut xx = x.borrow_mut();
        *xx = xx.wrapping_add(size);
    });
}

#[inline]
pub fn add_network_output_bytes(size: u64) {
    LOCAL_OUTPUT_BYTES.with(|x| x.borrow_mut().wrapping_add(size));
}

pub fn info_command(
    server: &mut Server,
    _client: Option<&mut Client>,
    _nodeid: u64,
    _uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter();
    let m = &server.metrics;
    let resp = match args.next_string() {
        Ok(part) => match part.to_ascii_lowercase().as_str() {
            "server" => format!("{}", m.server),
            "memory" => format!("{}", m.memory),
            "clients" => format!("{}", m.clients),
            "stats" => format!("{}", m.stats),
            "replication" => format!("{}", m.replication),
            "cpu" => format!("{}", m.cpu),
            "keyspace" => format!("{}", m.keyspace),
            o => return Err(CstError::UnknownSubCmd("info".to_string(), o.to_string())),
        },
        Err(_) => format!("{}", m),
    };
    Ok(Message::BulkString(resp.into()))
}
