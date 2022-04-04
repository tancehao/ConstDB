use crate::conn::Conn;
use std::collections::LinkedList;
use crate::cmd::{Cmd, NextArg};
use crate::server::{EventsConsumer, Server};
use std::thread::ThreadId;
use crate::stats::decr_clients;
use crate::link::{Link, LinkType};
use crate::{CstError, now_mil};
use crate::resp::Message;
use tokio::net::TcpStream;
use crate::conf::GLOBAL_CONF;
use tokio::time::sleep;
use tokio::time::Duration;

pub struct Client {
    conn: Conn,
    req: LinkedList<Cmd>,
    reply: LinkedList<Message>,
    pub(crate) last_write_uuid: Option<u64>,
    pub(crate) wait: Option<(u64, Option<u64>, u64, EventsConsumer<u64>)>, // quorum, timeout, uuid, consumer
    #[allow(unused)]
    pub thread_id: ThreadId,
    pub close: bool,
}

impl Drop for Client {
    fn drop(&mut self) {
        decr_clients();
    }
}

#[async_trait]
impl Link for Client {
    fn addr(&self) -> &str {
        self.conn.addr.as_str()
    }

    fn link_type(&self) -> LinkType {
        LinkType::Client
    }

    fn serve(&mut self, server: &mut Server) {
        if self.wait.is_some() {
            self.check_for_acks(server);
        }
        self.exec_cmds(server);
    }

    async fn prepare(&mut self) {
        if let Err(e) = self.prepare_client().await {
            self.conn.write_msg(Message::Error(e.into()));
            self.close = true;
        }
    }

    #[inline]
    fn to_serve(&self) -> bool {
        !self.req.is_empty()
    }

    fn to_close(&self) -> bool {
        self.close
    }

    async fn close(&mut self) -> Result<(), CstError> {
        while let Some(msg) = self.reply.pop_front() {
            self.conn.write_msg(msg);
        }
        self.conn.send_msg(Message::None).await
    }
}

impl Client {
    pub fn new(conn: TcpStream, addr: String) -> Self {
        let conn = Conn::new(Some(conn), addr);
        Self {
            conn,
            req: LinkedList::new(),
            close: false,
            last_write_uuid: None,
            wait: None,
            thread_id: std::thread::current().id(),
            reply: LinkedList::default(),
        }
    }

    pub fn take_conn(&mut self) -> Conn {
        let addr = self.addr().to_string();
        std::mem::replace(&mut self.conn, Conn::new(None, addr))
    }

    async fn prepare_client(&mut self) -> Result<(), CstError> {
        if self.close {
            return Ok(());
        }
        while let Some(res) = self.reply.pop_front() {
            self.conn.write_msg(res);
        }

        if let Some((_, timeout, uuid, e)) = &mut self.wait {
            match timeout {
                None => e.occured(uuid).await,
                Some(t) => {
                    tokio::select! {
                        _t = sleep(Duration::from_millis(*t)) => {},
                        _r = e.occured(uuid) => {},
                    };
                }
            }
            return Ok(())
        }
        let (readable, writable) = self.conn.net_ready().await?;
        if readable {
            if let Some(0) = self.conn.read_input()? {
                self.close = true;
            }
        }
        if self.conn.input_to_process() {
            while let Some(msg) = self.conn.try_next_msg()? {
                if let Err(e) = self.parse_cmd(msg) {
                    self.reply.push_back(Message::Error(e.into()));
                }
            }
        }
        if writable {
            self.conn.write_output()?;
        }

        Ok(())
    }

    fn parse_cmd(&mut self, msg: Message) -> Result<(), CstError> {
        match msg {
            Message::Array(args) => {
                self.req.push_back(Cmd::new(args)?);
                Ok(())
            }
            _ => Err(CstError::InvalidRequestMsg(
                format!("The resp message should be an array").into(),
            ))
        }
    }

    fn check_for_acks(&mut self, server: &mut Server) {
        match &self.wait {
            None => return,
            Some((quorum, timeout, uuid, _)) => {
                if let Some(t) = timeout {
                    let now = now_mil();
                    if *t < now {
                        self.reply.push_back(Message::Error("Timeout".into()));
                        self.wait = None;
                        return;
                    }
                }
                let mut res = vec![];
                server.replicas.iter(|x| {
                    if x.uuid_he_acked >= *uuid {
                        res.push(Message::BulkString(x.he.addr.as_str().into()));
                    }
                });
                if *quorum <= res.len() as u64 {
                    self.wait = None;
                    self.reply.push_back(Message::Array(res));
                }
            }
        }
    }

    fn exec_cmds(&mut self, server: &mut Server) {
        for _ in 0..GLOBAL_CONF.max_exec_per_round {
            if let Some(cmd) = self.req.pop_front() {
                let reply = match cmd.exec(Some(self), server) {
                    Ok(msg) => msg,
                    Err(e) => Message::Error(e.into()),
                };
                self.reply.push_back(reply);
            }
        }
    }
}


pub fn client_command(
    _server: &mut Server,
    client: Option<&mut Client>,
    _nodeid: u64,
    _uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let sub_command = args.next_string()?;
    match sub_command.to_ascii_lowercase().as_str() {
        "threadid" => {
            let tid = client.unwrap().thread_id;
            Ok(Message::BulkString(format!("{:?}", tid).into()))
        },
        "last_write_uuid" => {
            Ok(Message::Integer(client.unwrap().last_write_uuid.map(|x| x as i64).unwrap_or(-1)))
        }
        others => Err(CstError::UnknownSubCmd(
            others.to_string(),
            "CLIENT".to_string(),
        )),
    }
}

pub fn wait_command(
    server: &mut Server,
    client: Option<&mut Client>,
    _nodeid: u64,
    _uuid: u64,
    args: Vec<Message>,
) -> Result<Message, CstError> {
    let mut args = args.into_iter().skip(1);
    let mut timeout: Option<u64> = None;
    let mut quorum = 1;
    if let Ok(option) = args.next_string().map(|x| x.to_ascii_uppercase()) {
        match option.as_str() {
            "-TIMEOUT" => timeout = Some(args.next_u64()?),
            "-QUORUM" => quorum = args.next_u64()?,
            others => return Err(CstError::InvalidRequestMsg(format!("Unknown option for wait command {}", others))),
        }
    }
    if quorum == 0 {
        return Ok(Message::Error("Quorum is 0! We needn't wait".into()));
    }
    let client = client.unwrap();
    match client.last_write_uuid {
        None => Err(CstError::InvalidRequestMsg("No previous write command for waiting".into())),
        Some(uuid) => {
            let mut addrs = vec![];
            let mut total = 0;
            server.replicas.iter(|x| {
                total += 1;
                if x.uuid_he_acked > uuid {
                    addrs.push(Message::BulkString(x.he.addr.as_str().into()));
                }
            });
            if total < quorum {
                return Ok(Message::Error(format!("Quorum is too big! You want acks from {} replicas but we have only {} replicas", quorum, total).into()));
            }
            if addrs.len() as u64 <= quorum {
                client.wait = Some((quorum, timeout, uuid, server.new_replica_acked_event_watcher()));
                Ok(Message::None)
            } else {
                server.repl_backlog.replicate_cmd(uuid, "replurge", vec![]);
                Ok(Message::Array(addrs))
            }
        }
    }
}