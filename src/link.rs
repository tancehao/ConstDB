use std::collections::VecDeque;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::sync::mpsc::Sender;

use crate::CstError;
use crate::cmd::Cmd;
use crate::conn::Conn;
use crate::resp::Message;
use crate::server::{EventsConsumer, Server};
use crate::stats::decr_clients;
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


pub struct SharedLink(Arc<Mutex<Box<dyn Link + Send>>>);

impl<T> From<T> for SharedLink
	where T: Link + 'static + Send
{
	fn from(l: T) -> Self {
		Self(Arc::new(Mutex::new(Box::new(l))))
	}
}

impl SharedLink {
	pub async fn prepare(&mut self, s: Sender<OwnedMutexGuard<Box<dyn Link + Send>>>) {
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
				if s.send(l).await.is_err() {
					error!("Failed to send a link to main thread because the later is panicked!");
					break;
				}
			}
		}
	}
}

pub struct Client {
	pub conn: Conn,
	pub req: VecDeque<Message>,
	#[allow(unused)]
	events: Option<EventsConsumer>,
	pub close: bool,
	pub thread_id: ThreadId,
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
		match self.parse_cmd_and_exec(server) {
			Ok(Some(reply)) => self.conn.write_msg(reply),
			Ok(None) => {},
			Err(e) => self.conn.write_msg(Message::Error(e.into())),
		}
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

#[derive(Debug)]
pub enum LinkType {
	Client,
	Replica,
	Lua,
}

impl Client {
	pub fn new(conn: TcpStream, addr: String, events: EventsConsumer) -> Self {
		let conn = Conn::new(Some(conn), addr);
		Self{
			conn,
			req: VecDeque::new(),
			close: false,
			events: Some(events),
			thread_id: std::thread::current().id(),
		}
	}

	async fn prepare_client(&mut self) -> Result<(), CstError> {
		if self.close {
			return Ok(());
		}

		self.thread_id = std::thread::current().id();
		let (readable, writable) = self.conn.net_ready().await?;
		if readable {
			if let Some(0) = self.conn.read_input()? {
				self.close = true;
			}
		}
		if self.conn.input_to_process() {
			while let Some(msg) = self.conn.try_next_msg()? {
				self.req.push_back(msg);
			}
		}
		if writable {
			self.conn.write_output()?;
		}
		Ok(())
	}

	pub fn parse_cmd_and_exec(&mut self, server: &mut Server) -> Result<Option<Message>, CstError> {
		let args = match self.req.pop_front() {
			None => return Ok(None),
			Some(Message::Array(args)) => args,
			Some(e) => {
				return Err(CstError::InvalidRequestMsg(format!("The resp message should be an array instead of {:?}", e).into()))
			},
		};

		let cmd_name = match args.get(0) {
			None => return Err(CstError::WrongArity),
			Some(Message::BulkString(s)) => s,
			Some(_) => {
				// TODO need to tell the client what is wrong and then close it.
				error!("the command should be of array type of bulk strings");
				self.close = true;
				return Err(CstError::InvalidRequestMsg("the first argument should be of bulk string type".to_string()));
			}
		};
		let cmd = Cmd::new(cmd_name.as_bytes(), args[1..].to_vec())?;
		let reply = match cmd.exec(Some(self), server) {
			Ok(msg) => msg,
			Err(e) => Message::Error(e.into()),
		};
		Ok(Some(reply))
	}
}