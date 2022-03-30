use std::collections::VecDeque;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::cmd::Cmd;
use crate::conf::GLOBAL_CONF;
use crate::conn::Conn;
use crate::resp::Message;
use crate::server::Server;
use crate::server::ACTORS_QUEUE;
use crate::stats::decr_clients;
use crate::CstError;
use std::thread::ThreadId;

#[async_trait]
pub trait Link {
    // the remote addr
    fn addr(&self) -> &str;
    // the concrete type of this link
    fn link_type(&self) -> LinkType;
    // serve the link in the main thread, which serves all links in serialize
    fn serve(&mut self, server: &mut Server);
    // wait for some events after which we can interact with the peer
    async fn prepare(&mut self);
    // whether the link needs to be served in the main thread
    fn to_serve(&self) -> bool;
    // whether the link has been closed. If it is, we stops serving it and drops the connection
    fn to_close(&self) -> bool;
    async fn close(&mut self) -> Result<(), CstError>;
}

#[derive(Debug)]
pub enum LinkType {
    Client,
    Replica,
    Lua,
    StatsCollector,
}

#[derive(Clone)]
pub struct SharedLink(Arc<Mutex<Box<dyn Link + Send>>>);

impl<T> From<T> for SharedLink
where
    T: Link + 'static + Send,
{
    fn from(l: T) -> Self {
        Self(Arc::new(Mutex::new(Box::new(l))))
    }
}

impl SharedLink {
    pub async fn prepare(&mut self) {
        let sender = ACTORS_QUEUE.get().unwrap().clone();
        loop {
            let mut l = self.0.clone().lock_owned().await;
            l.prepare().await;
            if l.to_close() {
                if let Err(e) = l.close().await {
                    error!("Failed to close link with {} because {}", l.addr(), e);
                }
                break;
            }
            if l.to_serve() {
                if sender.send(l).await.is_err() {
                    error!("Failed to send a link to main thread because the later is panicked!");
                    break;
                }
            }
        }
    }
}

pub struct Client {
    conn: Conn,
    req: VecDeque<Cmd>,
    reply: VecDeque<Message>,
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
        self.conn.send_msg(Message::None).await
    }
}

impl Client {
    pub fn new(conn: TcpStream, addr: String) -> Self {
        let conn = Conn::new(Some(conn), addr);
        Self {
            conn,
            req: VecDeque::new(),
            close: false,
            thread_id: std::thread::current().id(),
            reply: Default::default(),
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
        let args = match msg {
            Message::Array(args) => args,
            _ => {
                return Err(CstError::InvalidRequestMsg(
                    format!("The resp message should be an array").into(),
                ))
            }
        };
        let cmd_name = match args.first() {
            None => return Err(CstError::WrongArity),
            Some(Message::BulkString(s)) => s,
            Some(_) => {
                self.close = true;
                return Err(CstError::InvalidRequestMsg(
                    "the first argument should be of bulk string type".to_string(),
                ));
            }
        };

        let cmd = Cmd::new(cmd_name.as_bytes(), args[1..].to_vec())?;
        self.req.push_back(cmd);
        Ok(())
    }

    pub fn exec_cmds(&mut self, server: &mut Server) {
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
