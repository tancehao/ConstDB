use std::cmp::min;

use tokio::io;

use crate::CstError;
use crate::lib::utils::bytes2i64;
use crate::resp::Message;
use crate::stats::add_network_input_bytes;

pub const DEFAULT_CLIENT_INPUT_BUF: usize = 128;

#[derive(Default, Debug)]
pub struct ReadBuf {
    pub addr: String,
    pub(crate) input_buf: Vec<u8>,  // input buffer
    pub(crate) input_pos: usize,    // the position
    pub(crate) input_parsed: usize,
    pub(crate) input_total: usize,
    pub(crate) parsed_total: usize,

}

pub const RESP_TYPE_STRING: u8 = b'+';
pub const RESP_TYPE_ERR: u8 = b'-';
pub const RESP_TYPE_INTEGER: u8 = b':';
pub const RESP_TYPE_BULK: u8 = b'$';
pub const RESP_TYPE_ARRAY: u8 = b'*';

impl ReadBuf {
    pub fn new(addr: String) -> Self {
        let mut c = Self{
            addr,
            input_buf: Vec::with_capacity(DEFAULT_CLIENT_INPUT_BUF),
            input_pos: 0,
            input_parsed: 0,

            input_total: 0,
            parsed_total: 0
        };
        c.input_buf.resize(DEFAULT_CLIENT_INPUT_BUF, 0);
        c
    }

    // the source at this buffer from where bytes could be read to be decoded.
    pub fn buf_reader(&self) -> &[u8] {
        self.input_buf[self.input_parsed..self.input_pos].as_ref()
    }

    pub fn try_read<T: TryReader>(&mut self, s: &T) -> io::Result<usize> {
        let r = self.input_buf[self.input_pos..].as_mut();
        // TODO need to check r's length, the try_read will return Ok(0) if r's length is 0;
        let r = s.try_read(r);
        if let Ok(s) = r {
            self.input_pos += s;
            self.input_total += s;
        }
        r
    }

    pub fn grow_input_buf(&mut self) {
        // TODO
        self.input_buf.resize(self.input_buf.len()*2, 0);
    }

    #[inline]
    pub fn total_read(&self) -> usize {
        self.parsed_total
    }

    #[inline]
    pub fn readable_room(&self) -> usize {
        self.input_buf.capacity() - self.input_pos
    }

    #[inline]
    pub fn io_readable_size(&self) -> usize {
        self.input_buf.capacity() - self.input_pos
    }

    fn after_parsed(&mut self) {
        if self.input_parsed == self.input_pos {
            self.input_parsed = 0;
            self.input_pos = 0;
        } else if self.input_pos == self.input_buf.len()+1 && (self.input_pos-self.input_parsed) <= self.input_pos / 2 {
            let (left, right) = self.input_buf.split_at_mut(self.input_parsed);
            left[..(self.input_pos-self.input_parsed)].copy_from_slice(right);
            self.input_pos = self.input_parsed;
            self.input_parsed = 0;
        }
    }

    pub fn forward(&mut self, size: usize) {
        self.input_parsed += size;
        self.parsed_total += size;
        self.after_parsed();
    }

    pub fn unprocessed_size(&self) -> usize {
        self.input_pos - self.input_parsed
    }

    pub fn next_msg(&mut self) -> Result<Option<Message>, CstError> {
        if self.input_parsed == self.input_pos {
            return Ok(None);
        }
        let (msg, size) = self.parse_msg_inner(self.input_parsed)?;
        self.input_parsed += size;
        self.parsed_total += size;
        self.after_parsed();
        Ok(Some(msg))
    }

    // parse the resp-formated data into a valid message, along with witch we returns its total size too.
    fn parse_msg_inner(&mut self, cursor: usize) -> Result<(Message, usize), CstError> {
        if cursor >= self.input_pos {
            return Err(CstError::NeedMoreMsg);
        }
        match self.input_buf.get_mut(cursor) {
            None => Err(CstError::NeedMoreMsg),
            Some(t) => {
                match *t {
                    RESP_TYPE_STRING => {
                        let s = self.read_until_crcf(cursor+1)?;
                        Ok((Message::String(self.input_buf[cursor+1..s-1].into()), s-cursor+1))
                    }
                    RESP_TYPE_INTEGER => {
                        let s = self.read_until_crcf(cursor+1)?;
                        match bytes2i64(self.input_buf[cursor+1..s-1].as_ref()) {
                            Some(i) => Ok((Message::Integer(i), s-cursor+1)),
                            None => Err(CstError::InvalidRequestMsg("the ':' should be followed by an integer".to_string()))
                        }
                    }
                    RESP_TYPE_ERR => {
                        let s = self.read_until_crcf(cursor+1)?;
                        Ok((Message::Error(self.input_buf[cursor+1..s-1].into()), s-cursor+1))
                    }
                    RESP_TYPE_BULK => {
                        let (str_begin, str_end) = self.read_bulk_string(cursor)?;
                        let msg = match str_begin {
                            None => Message::Nil,
                            Some(s) => Message::BulkString(self.input_buf[s..str_end-1].into()),
                        };
                        Ok((msg, str_end - cursor + 1))
                    }
                    RESP_TYPE_ARRAY => {
                        let array_len_end = self.read_until_crcf(cursor+1)?;
                        match bytes2i64(self.input_buf[cursor+1..array_len_end-1].as_ref()) {
                            None => {
                                Err(CstError::InvalidRequestMsg("the '*' should be followed by an integer".to_string()))
                            },
                            Some(array_len) => {
                                let mut args = Vec::with_capacity(array_len as usize);
                                let mut sub_cursor = array_len_end+1;
                                for _ in 0..array_len {
                                    let (msg, size) = self.parse_msg_inner(sub_cursor)?;
                                    args.push(msg);
                                    sub_cursor += size;
                                }
                                Ok((Message::Array(args), sub_cursor - cursor))
                            }
                        }
                    }
                    other => {
                        error!("unknown resp type {}, buf: {:?}", other, self.input_buf[cursor..].as_ref());
                        Err(CstError::InvalidRequestMsg(format!("unknown resp type {}", other)))
                    }
                }
            }
        }
    }

    fn read_bulk_string(&self, cursor: usize) -> Result<(Option<usize>, usize), CstError> {
        match self.input_buf.get(cursor) {
            None => Err(CstError::NeedMoreMsg),
            Some(t) => if *t != RESP_TYPE_BULK {
                Err(CstError::InvalidRequestMsg("should be of bulk string type".to_string()))
            } else {
                let header_end = self.read_until_crcf(cursor+1)?;
                match bytes2i64(self.input_buf[cursor+1..header_end-1].as_ref()) {
                    None => {
                        Err(CstError::InvalidRequestMsg("the '$' should be followed by an integer".to_string()))
                    },
                    Some(char_cnt) => {
                        if char_cnt == -1 {
                            return Ok((None, cursor + 4));
                        }
                        if char_cnt < 0 {
                            return Err(CstError::InvalidRequestMsg("invalid bulk string".to_string()));
                        }
                        let str_end = self.read_until_crcf(header_end)?;
                        if str_end - header_end != char_cnt as usize + 2 {
                            return Err(CstError::InvalidRequestMsg("bulk string has wrong value of length".to_string()));
                        }
                        Ok((Some(header_end + 1), str_end))
                    }
                }
            }
        }
    }

    // return the first absolute position where "\r\n" exists after cursor in the `input_buf`
    pub fn read_until_crcf(&self, cursor: usize) -> Result<usize, CstError> {
        let len = self.input_buf.len();
        for i in cursor..len {
            if self.input_buf[i] == b'\r' && i < len - 1 && self.input_buf[i+1] == b'\n' {
                return Ok(i + 1);
            }
        }
        Err(CstError::NeedMoreMsg)
    }

}

pub trait TryReader {
    fn try_read(&self, buf: &mut [u8]) -> io::Result<usize>;
}

impl TryReader for tokio::net::TcpStream {
    fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let r = tokio::net::TcpStream::try_read(self, buf);
        if let Ok(s) = r {
            add_network_input_bytes(s as u64);
        }
        r
    }
}

impl TryReader for tokio::net::tcp::OwnedReadHalf {
    fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let r = tokio::net::TcpStream::try_read(self.as_ref(), buf);
        if let Ok(s) = r {
            add_network_input_bytes(s as u64);
        }
        r
    }
}

impl TryReader for Vec<u8> {
    fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let l = min(buf.len(), self.len());
        buf[0..l].as_mut().copy_from_slice(self.as_slice()[0..l].as_ref());
        Ok(l)
    }
}
