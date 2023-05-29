use std::sync::Arc;

use tokio::sync::Mutex;

use crate::server::Server;
use crate::server::ACTORS_QUEUE;
use crate::CstError;
use async_trait::async_trait;
use log::*;

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
