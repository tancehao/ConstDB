use clap::load_yaml;
use lazy_static::lazy_static;
use serde_derive::Deserialize;
use std::env::current_dir;

use clap::App;
use std::net::SocketAddrV4;

lazy_static! {
    pub static ref GLOBAL_CONF: Config = parse_args();
    pub static ref CONF_PATH: String = get_conf_path();
}

#[derive(Debug, Clone)]
pub struct Config {
    pub daemon: bool,
    pub node_id: u64,
    pub node_alias: String,
    pub ip: String,
    pub port: u16,
    pub addr: String,
    pub threads: usize,
    pub log: String,
    pub work_dir: String,
    pub tcp_backlog: u32,
    pub repl_backlog_limit: u64,
    pub replica_heartbeat_frequency: u32,
    pub replica_gossip_frequency: u32,
    pub log_level: String,
    pub max_exec_per_round: u32,
    pub hz: u16,
}

impl Config {
    fn validate(&self) -> Result<(), String> {
        match self.log_level.as_str() {
            "OFF" | "TRACE" | "DEBUG" | "INFO" | "WARN" | "ERROR" => {},
            _ => return Err("log_level should be one of \"OFF\", \"TRACE\", \"DEBUG\", \"INFO\", \"WARN\", \"ERROR\"".to_string())
        }
        if self.addr.parse::<SocketAddrV4>().is_err() {
            return Err(format!(
                "address {} is not a valid ipv4 socket address",
                self.addr
            ));
        }
        // TODO
        Ok(())
    }
}

#[derive(Deserialize)]
struct OriginConfig {
    daemon: Option<bool>,
    node_id: u64,
    node_alias: String,
    ip: Option<String>,
    port: Option<u16>,
    log: Option<String>,
    work_dir: Option<String>,
    threads: Option<usize>,
    tcp_backlog: Option<u32>,
    repl_backlog_limit: Option<u64>,
    replica_heartbeat_frequency: Option<u32>,
    replica_gossip_frequency: Option<u32>,
    log_level: Option<String>,
    max_exec_per_round: Option<u32>,
    hz: Option<u16>,
}

fn get_conf_path() -> String {
    let yaml = load_yaml!("server.yml");
    let matches = App::from_yaml(yaml).version("1.0.0").get_matches();
    match matches.value_of("config") {
        Some(conf) => conf.to_string(),
        None => match std::env::current_dir() {
            Ok(d) => d.join("constdb.toml").to_str().unwrap().to_string(),
            Err(_) => {
                println!("config file is not specified and failed to load the default one because we can't get path of current directory");
                std::process::exit(-1);
            }
        },
    }
}

fn parse_args() -> Config {
    match std::fs::read_to_string(&*CONF_PATH) {
        Err(e) => {
            println!("we are not able to load the config because {}, use -config flag to specify a new one", e);
            std::process::exit(-1);
        }
        Ok(confs) => match toml::from_str::<OriginConfig>(confs.as_str()) {
            Err(e) => {
                println!("the config file should be of toml type. Err: {}", e);
                std::process::exit(-1);
            }
            Ok(oc) => {
                let workdir = oc
                    .work_dir
                    .unwrap_or_else(|| current_dir().unwrap().to_str().unwrap().to_string());
                let ip = oc.ip.unwrap_or("0.0.0.0".to_string());
                let port = oc.port.unwrap_or(9001);
                let c = Config {
                    daemon: oc.daemon.unwrap_or_default(),
                    node_id: oc.node_id,
                    node_alias: oc.node_alias,
                    addr: format!("{}:{}", ip, port),
                    ip,
                    port,
                    work_dir: workdir.clone(),
                    log: oc.log.unwrap_or_default(),
                    log_level: oc.log_level.unwrap_or("INFO".to_string()).to_uppercase(),
                    tcp_backlog: oc.tcp_backlog.unwrap_or(1024),
                    replica_heartbeat_frequency: oc.replica_heartbeat_frequency.unwrap_or(40),
                    replica_gossip_frequency: oc.replica_gossip_frequency.unwrap_or(15),
                    threads: oc.threads.unwrap_or(4),
                    repl_backlog_limit: oc.repl_backlog_limit.unwrap_or(1024 * 1024 * 50),
                    max_exec_per_round: oc.max_exec_per_round.unwrap_or(16),
                    hz: oc.hz.unwrap_or(100),
                };
                if let Err(e) = c.validate() {
                    panic!("Invalid config. {}", e);
                }
                c
            }
        },
    }
}
