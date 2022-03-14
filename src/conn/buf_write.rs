use tokio::io;
use crate::Bytes;
use crate::resp::Message;
use crate::stats::add_network_output_bytes;

pub const DEFAULT_CLIENT_OUTPUT_BUF: usize = 128;
pub const RESP_TYPE_STRING: u8 = b'+';
pub const RESP_TYPE_ERR: u8 = b'-';
pub const RESP_TYPE_INTEGER: u8 = b':';
pub const RESP_TYPE_BULK: u8 = b'$';
pub const RESP_TYPE_ARRAY: u8 = b'*';

thread_local!{
    pub static INT_BYTES: Vec<Bytes> = {
        let mut m = Vec::new();
        for i in 0..10001 {
            m.push(format!("{}", i-1).into());
        }
        m
    };
}

pub fn get_int_bytes(n: i64) -> Bytes {
    if n >= -1 && n < 10000 {
        INT_BYTES.with(|x| x[(n+1) as usize].clone())
    } else {
        format!("{}", n).into()
    }
}

#[derive(Default, Debug)]
pub struct WriteBuf {
    pub addr: String,
    pub(crate) reply_buf: Vec<u8>,
    pub(crate) reply_pos: usize,
    pub(crate) replied: usize,
    pub(crate) replied_total: usize,
}

impl WriteBuf {
    pub fn new(addr: String) -> Self {
        let mut c = Self{
            addr,

            reply_pos: 0,
            reply_buf: Vec::with_capacity(DEFAULT_CLIENT_OUTPUT_BUF),
            replied: 0,
            replied_total: 0,
        };
        c.reply_buf.resize(DEFAULT_CLIENT_OUTPUT_BUF, 0);
        c
    }

    // the destination at this buffer into where bytes could be written into.
    pub fn buf_writer(&mut self) -> &mut [u8] {
        self.reply_buf[self.reply_pos..].as_mut()
    }

    pub fn try_write<T: TryWriter>(&mut self, s: &mut T) -> io::Result<usize> {
        //println!("conn {}, trying write, reply_buf: {:?}, self.replied: {}, self.reply_pos: {}", self.addr, String::from_utf8(self.reply_buf.as_slice().to_vec()), self.replied, self.reply_pos);
        let w = self.reply_buf[self.replied..self.reply_pos].as_ref();
        //println!("conn {}, trying write: {:?}", self.addr, String::from_utf8(w.to_vec()));

        let r = s.try_write(w);
        if let Ok(size) = r {
            self.replied += size;
            if self.replied == self.reply_pos {
                self.replied = 0;
                self.reply_pos = 0;
                //println!("conn: {}, reply_pos was set to 0", self.addr);
            }
            self.replied_total += size;
        }
        r
    }

    #[inline]
    pub fn write_bytes(&mut self, d: &[u8]) -> &mut Self {
        //println!("conn: {}, writing bytes: {:?}", self.addr, String::from_utf8(d.to_vec()));
        let size = d.len();
        if self.reply_pos + size > self.reply_buf.len() {
            let mut s = self.reply_buf.len();
            while self.reply_pos + size >= s {
                s = if s < 2048 {
                    s * 2
                } else {
                    s * 3 / 4
                };
            }
            self.reply_buf.resize(s, 0);
        }
        self.reply_buf[self.reply_pos..self.reply_pos+size].copy_from_slice(d);
        self.reply_pos += size;
        //println!("conn: {}, reply_buf after copy_from_slice: {:?}, reply_pos: {}, replied: {}", self.addr, String::from_utf8(self.reply_buf.to_vec()), self.reply_pos, self.replied);
        self
    }

    #[inline]
    pub fn write_integer(&mut self, n: i64) -> &mut Self {
        self.write_bytes(get_int_bytes(n).as_bytes())
    }

    #[inline]
    pub fn total_write(&self) -> usize {
        self.replied_total
    }

    #[inline]
    pub fn writable_room(&self) -> usize {
        self.reply_buf.capacity() - self.reply_pos
    }

    #[inline]
    pub fn io_writable_size(&self) -> usize {
        self.reply_pos - self.replied
    }

    pub fn add_replied_size(&mut self, s: usize) {
        self.replied_total += s;
    }
}

//
// resp
//
impl WriteBuf {
    pub fn write_msg(&mut self, msg: Message) {
        //println!("conn: {}, writing Message: {}", self.addr, msg);
        match msg {
            Message::None => {},
            Message::Nil => {
                self.write_bytes(b"$-1\r\n");
            },
            Message::BulkString(s) => {
                self.write_bytes(b"$").write_integer(s.len() as i64).write_crcf().write_bytes(s.as_bytes()).write_crcf();
            }
            Message::Error(e) => {
                self.write_bytes(b"-").write_bytes(e.as_bytes()).write_crcf();
            }
            Message::String(s) => {
                self.write_bytes(b"+").write_bytes(s.as_bytes()).write_crcf();
            }
            Message::Integer(i) => {
                self.write_bytes(b":").write_integer(i).write_crcf();
            }
            Message::Array(msgs) => {
                self.write_bytes(b"*").write_integer(msgs.len() as i64).write_crcf();
                for msg in msgs {
                    self.write_msg(msg);
                }
            },
        }
    }

    #[inline]
    fn write_crcf(&mut self) -> &mut Self {
        self.write_bytes(b"\r\n")
    }
}

pub trait TryWriter {
    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize>;
}

impl TryWriter for tokio::net::TcpStream {
    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let r = tokio::net::TcpStream::try_write(self, buf);
        if let Ok(s) = r {
            add_network_output_bytes(s as u64);
        }
        r
    }
}

impl TryWriter for tokio::net::tcp::OwnedWriteHalf {
    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let r = tokio::net::TcpStream::try_write(self.as_ref(), buf);
        if let Ok(s) = r {
            add_network_output_bytes(s as u64);
        }
        r
    }
}

impl TryWriter for Vec<u8> {
    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        for i in 0..self.capacity() {
            self.push(buf[i]);
        }
        Ok(self.len())
    }
}
