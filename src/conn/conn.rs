use tokio::fs::File;
use tokio::net::TcpStream;

use crate::conn::reader::Reader;
use crate::conn::writer::Writer;
use crate::CstError;
use crate::resp::Message;

#[derive(Default, Debug)]
pub struct Conn {
    pub addr: String,
    writer: Writer,
    reader: Reader,
}

impl Drop for Conn {
    fn drop(&mut self) {
        debug!("Dropping conn with {}", self.addr);
    }
}

impl Conn {
    pub fn new(conn: Option<TcpStream>, addr: String) -> Self {
        let (puller, pusher) = match conn {
            None => (None, None),
            Some(c) => {
                let (rx, tx) = c.into_split();
                (Some(rx), Some(tx))
            }
        };
        Self{
            writer: Writer::new(addr.clone(), pusher),
            reader: Reader::new(addr.clone(), puller),
            addr
        }
    }

    pub fn split(&mut self) -> (Reader, Writer) {
        let (mut r, mut w) = (Reader::default(), Writer::default());
        std::mem::swap(&mut r, &mut self.reader);
        std::mem::swap(&mut w, &mut self.writer);
        (r, w)
    }
    
    
    pub fn from_splitted(r: &mut Reader, w: &mut Writer) -> Self {
        let mut c = Conn::new(None, r.addr.clone());
        std::mem::swap(&mut c.writer, w);
        std::mem::swap(&mut c.reader, r);
        c
    }

    pub async fn net_ready(&self) -> Result<(bool, bool), CstError> {
        let (mut readable, mut writable) = (false, false);
        let (readable_size, writable_size) = (self.reader.io_readable_size(), self.writer.io_writable_size());
        tokio::select! {
            r = self.reader.readable(), if readable_size > 0 => {
                r?;
                readable = true;
            }
            w = self.writer.writable(), if writable_size > 0 => {
                w?;
                writable = true;
            }
        }
        return Ok((readable, writable));
    }

    pub fn set_connection(&mut self, conn: TcpStream) {
        let (rx, tx) = conn.into_split();
        self.writer.set_connection(tx);
        self.reader.set_connection(rx);
    }

    pub async fn readable(&self) -> Result<(), CstError> {
        self.reader.readable().await
    }

    pub async fn writable(&self) -> Result<(), CstError> {
        self.writer.writable().await
    }

    pub fn write_output(&mut self) -> Result<Option<usize>, CstError> {
        self.writer.write_output()
    }

    pub fn read_input(&mut self) -> Result<Option<usize>, CstError> {
        self.reader.read_input()
    }

    pub fn input_to_process(&self) -> bool {
        self.reader.input_to_process()
    }

    pub async fn next_msg(&mut self) -> Result<Message, CstError> {
        self.reader.next_msg().await
    }

    pub fn try_next_msg(&mut self) -> Result<Option<Message>, CstError> {
        self.reader.try_next_msg()
    }

    pub fn write_msg(&mut self, msg: Message) {
        self.writer.write_msg(msg)
    }

    pub async fn send_msg(&mut self, msg: Message) -> Result<(), CstError> {
        self.writer.send_msg(msg).await
    }

    #[inline]
    pub fn writable_room(&self) -> usize {
        self.writer.writable_room()
    }

    #[inline]
    pub fn io_writable_size(&self) -> usize {
        self.writer.io_writable_size()
    }

    #[inline]
    pub fn readable_room(&self) -> usize {
        self.reader.readable_room()
    }

    #[inline]
    pub fn io_readable_size(&self) -> usize {
        self.reader.io_readable_size()
    }

    pub async fn send_file(&mut self, src: &mut File) -> Result<(), CstError> {
        self.writer.send_file(src).await
    }
}

#[cfg(test)]
mod test {
    use tokio::macros::support::thread_rng_n;

    use crate::resp::Message;
    use crate::conn::buf_read::ReadBuf;
    use crate::conn::buf_write::WriteBuf;

    #[test]
    fn test_io_buf() {
        let msgs = vec![
            Message::String("String".into()),
            Message::Integer(100),
            Message::Error("Error".into()),
            Message::BulkString("Bulk".into()),
            Message::Nil,
            Message::BulkString("".into()),
            Message::Array(vec![
                Message::BulkString("CMD".into()),
                Message::String("arg1".into()),
                Message::Error("arg2".into()),
                Message::Integer(3),
                Message::Array(vec![Message::BulkString("arg4".into())]),
            ])
        ];

        let bs = vec![
            "+String\r\n",
            ":100\r\n",
            "-Error\r\n",
            "$4\r\nBulk\r\n",
            "$-1\r\n",
            "$0\r\n\r\n",
            "*5\r\n$3\r\nCMD\r\n+arg1\r\n-arg2\r\n:3\r\n*1\r\n$4\r\narg4\r\n"
        ];

        let mut buf = ReadBuf::new("test".to_string());
        buf.input_buf.resize(10240, 0);
        let mut indices = vec![];
        for i in 0..100 {
            let j = thread_rng_n(4) as usize;
            indices.push(j);
            buf.try_read(&bs[j].as_bytes().to_vec());
        }

        for i in indices {
            assert_eq!(buf.next_msg().unwrap(), Some(msgs[i].clone()));
        }

        let mut buf = WriteBuf::new("test1".to_string());
        buf.reply_buf.resize(10240, 0);
        let mut indices = vec![];
        for i in 0..100 {
            let j = thread_rng_n(4) as usize;
            indices.push(j);
            buf.write_msg(msgs[j].clone());
        }

        for i in indices {
            let m = bs[i];
            let mut dd: Vec<u8> = Vec::with_capacity(m.len());
            let s = buf.try_write(&mut dd).unwrap();
            assert_eq!(s, m.len());
            assert_eq!(dd[0..s].as_ref(), m.as_bytes());
        }
    }
}