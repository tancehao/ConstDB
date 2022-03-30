use tokio::fs::File;
use tokio::io::ErrorKind;
use tokio::net::tcp::OwnedWriteHalf;

use crate::conn::buf_write::WriteBuf;
use crate::resp::Message;
use crate::CstError;

#[derive(Debug, Default)]
pub struct Writer {
    pub addr: String,
    pub(crate) conn: Option<OwnedWriteHalf>,
    pub(crate) write_buf: WriteBuf,
}

impl Writer {
    pub fn new(addr: String, conn: Option<OwnedWriteHalf>) -> Self {
        Self {
            conn,
            write_buf: WriteBuf::new(addr.clone()),
            addr,
        }
    }

    #[inline]
    pub fn set_connection(&mut self, conn: OwnedWriteHalf) {
        self.conn = Some(conn);
    }

    pub async fn writable(&self) -> Result<(), CstError> {
        let c = self.conn.as_ref().ok_or(CstError::SystemError)?;
        c.as_ref().writable().await.map_err(|x| x.into())
    }

    pub fn write_output(&mut self) -> Result<Option<usize>, CstError> {
        match &mut self.conn {
            Some(conn) => match self.write_buf.try_write(conn) {
                Ok(s) => {
                    debug!("io layer, wrote {} bytes to {}", s, self.addr);
                    Ok(Some(s))
                }
                Err(e) => {
                    if e.kind() != ErrorKind::WouldBlock {
                        error!("error: {}", e);
                        return Err(CstError::from(e));
                    }
                    Ok(None)
                }
            },
            None => Err(CstError::SystemError),
        }
    }

    pub fn write_msg(&mut self, msg: Message) {
        info!("buf layer, write msg {} to {}", msg, self.addr);
        self.write_buf.write_msg(msg)
    }

    pub async fn send_msg(&mut self, msg: Message) -> Result<(), CstError> {
        self.write_msg(msg);
        while self.io_writable_size() > 0 {
            self.writable().await?;
            if let Some(0) = self.write_output()? {
                return Err(CstError::ConnBroken(self.addr.clone()));
            }
        }
        Ok(())
    }

    #[inline]
    pub fn writable_room(&self) -> usize {
        self.write_buf.writable_room()
    }

    #[inline]
    pub fn io_writable_size(&self) -> usize {
        self.write_buf.io_writable_size()
    }

    #[inline]
    pub fn write_buf_cap(&self) -> usize {
        self.write_buf.reply_buf.capacity()
    }

    pub async fn flush(&mut self) -> Result<(), CstError> {
        while self.io_writable_size() > 0 {
            debug!("self.io_writable_size: {}", self.io_writable_size());
            if self.write_output()?.is_none() {
                self.writable().await?;
            }
        }
        Ok(())
    }

    pub async fn send_file(&mut self, src: &mut File) -> Result<(), CstError> {
        self.flush().await?;
        match self.conn.as_mut() {
            None => Err(CstError::SystemError),
            Some(conn) => match tokio::io::copy(src, conn).await {
                Ok(s) => {
                    debug!("In send_file, sent {} bytes into socket", s);
                    self.write_buf.add_replied_size(s as usize);
                    Ok(())
                }
                Err(e) => {
                    error!("Failed to send the snapshot into the socket because {}", e);
                    Err(CstError::ConnBroken(self.addr.clone()))
                }
            },
        }
    }
}
