use std::cmp::min;

use tokio::fs::File;
use tokio::io::{AsyncWriteExt, ErrorKind};
use tokio::net::tcp::OwnedReadHalf;

use crate::conn::buf_read::ReadBuf;
use crate::CstError;
use crate::resp::Message;

#[derive(Debug, Default)]
pub struct Reader {
    pub addr: String,
    pub(crate) conn: Option<OwnedReadHalf>,
    pub(crate) read_buf: ReadBuf,
}

impl Reader {
    pub fn new(addr: String, conn: Option<OwnedReadHalf>) -> Self {
        Self{
            conn,
            read_buf: ReadBuf::new(addr.clone()),
            addr,
        }
    }

    #[inline]
    pub fn set_connection(&mut self, conn: OwnedReadHalf) {
        self.conn = Some(conn);
    }

    pub async fn readable(&self) -> Result<(), CstError> {
        let c = self.conn.as_ref().ok_or(CstError::SystemError)?;
        c.as_ref().readable().await.map_err(|x| x.into())
    }

    pub fn read_input(&mut self) -> Result<Option<usize>, CstError> {
        match &self.conn {
            Some(conn) => {
                match self.read_buf.try_read(conn) {
                    Ok(size) => {
                        debug!("io layer, read {} bytes into input_buf from {}", size, self.addr);
                        Ok(Some(size))
                    },
                    Err(e) => {
                        if e.kind() != ErrorKind::WouldBlock {
                            error!("error: {}", e);
                            return Err(CstError::from(e));
                        }
                        Ok(None)
                    }
                }
            }
            None => Err(CstError::SystemError)
        }
    }

    pub fn input_to_process(&self) -> bool {
        self.read_buf.unprocessed_size() > 0
    }

    pub async fn next_msg(&mut self) -> Result<Message, CstError> {
        loop {
            match self.try_next_msg()? {
                Some(msg) => return Ok(msg),
                None => {
                    self.readable().await?;
                    if let Some(0) = self.read_input()? {
                        return Err(CstError::ConnBroken(self.addr.clone()));
                    }
                }
            }
        }
    }

    pub fn try_next_msg(&mut self) -> Result<Option<Message>, CstError> {
        match self.read_buf.next_msg() {
            Ok(Some(m)) => {
                debug!("buf layer, read msg {} from {}", m, self.addr);
                Ok(Some(m))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                debug!("next_msg failed from {} because {}", self.addr, e);
                if let CstError::NeedMoreMsg = e {
                    self.read_buf.grow_input_buf();
                    return Ok(None);
                }
                Err(e)
            }
        }
    }

    #[inline]
    pub fn readable_room(&self) -> usize {
        self.read_buf.readable_room()
    }

    #[inline]
    pub fn io_readable_size(&self) -> usize {
        self.read_buf.io_readable_size()
    }

    pub async fn save_to_file(&mut self, target: &mut File, mut size: usize) -> Result<(), CstError> {
        debug!("Saving snapshot into a file, size: {}", size);
        loop {
            let buf = self.read_buf.buf_reader();
            let wl = min(buf.len(), size);
            let buflen = target.write(buf[..wl].as_ref()).await?;
            debug!("Wrote {} bytes into snapshot file", buflen);
            self.read_buf.forward(wl);
            size -= buflen;
            if size == 0 {
                break;
            }
            self.readable().await?;
            let _ = self.read_input()?;
        }
        target.flush().await?;
        Ok(())
    }
}
