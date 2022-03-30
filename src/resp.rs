use std::fmt::Display;
use std::fmt::Formatter;

use crate::Bytes;

thread_local! {
    pub static MSG_OK: Message = Message::String("OK".into());
}

thread_local! {
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
        INT_BYTES.with(|x| x[(n + 1) as usize].clone())
    } else {
        format!("{}", n).into()
    }
}

#[inline]
pub fn new_msg_ok() -> Message {
    MSG_OK.with(|x| x.clone())
}

#[derive(Debug, Clone)]
pub enum Message {
    None,
    Nil,
    String(Bytes),
    Integer(i64),
    Error(Bytes),
    BulkString(Bytes),
    Array(Vec<Message>),
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Message::None => f.write_str("[NONE]"),
            Message::Nil => f.write_str("(nil)"),
            Message::Array(msgs) => {
                f.write_str("Arr(")?;
                let a = msgs
                    .into_iter()
                    .map(|x| format!("{}", x))
                    .collect::<Vec<String>>()
                    .join(",");
                f.write_str(a.as_str())?;
                f.write_str(")")
            }
            Message::String(s) => {
                f.write_str("Str(")?;
                f.write_fmt(format_args!("\x1b[1;33m{}\x1b[0m", String::from(s.clone())))?;
                f.write_str(")")
            }
            Message::Integer(s) => {
                f.write_str("Int(")?;
                //f.write_str(format!("{}", *s).as_str());
                f.write_fmt(format_args!("\x1b[1;33m{}\x1b[0m", *s))?;
                f.write_str(")")
            }
            Message::Error(s) => {
                f.write_str("ERR(")?;
                //f.write_str(String::from(s.clone()).as_str());
                f.write_fmt(format_args!("\x1b[1;33m{}\x1b[0m", String::from(s.clone())))?;
                f.write_str(")")
            }
            Message::BulkString(s) => {
                f.write_str("Bulk(")?;
                f.write_fmt(format_args!("\x1b[1;33m{}\x1b[0m", String::from(s.clone())))?;
                f.write_str(")")
            }
        }
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Message::None, Message::None) => true,
            (Message::String(l), Message::String(r)) => l == r,
            (Message::BulkString(l), Message::BulkString(r)) => l == r,
            (Message::Integer(l), Message::Integer(r)) => l == r,
            (Message::Error(l), Message::Error(r)) => l == r,
            (Message::Array(l), Message::Array(r)) => l == r,
            (Message::Nil, Message::Nil) => true,
            _ => false,
        }
    }
}

impl Message {
    pub fn size(&self) -> usize {
        match self {
            Message::None => 0,
            Message::Nil => 0,
            Message::String(b) => b.len(),
            Message::Error(e) => e.len(),
            Message::BulkString(b) => b.len(),
            Message::Integer(_) => 8,
            Message::Array(args) => args.iter().map(|x| x.size()).sum(),
        }
    }

    pub fn raw_size(&self) -> usize {
        match self {
            Message::None => 0,
            Message::Nil => 5,
            Message::String(b) => b.len() + 3,
            Message::Error(e) => e.len() + 3,
            Message::BulkString(b) => {
                let l = b.len();
                get_int_bytes(l as i64).len() + l + 5
            }
            Message::Integer(i) => get_int_bytes(*i).len() + 3,
            Message::Array(args) => {
                let l = args.len();
                let ds: usize = args.iter().map(|x| x.size()).sum();
                get_int_bytes(l as i64).len() + 3 + ds
            }
        }
    }
}

#[macro_export]
macro_rules! mkcmd {
    ($cmd:expr) => ({
        let mut args = vec![Message::BulkString(format!("{}", $cmd).into())];
        Message::Array(args)
    });
    ($cmd:expr, $($arg:expr),*) => ({
        let mut args = vec![Message::BulkString(format!("{}", $cmd).into())];
        $(
            args.push(Message::BulkString(format!("{}", $arg).into()));
        )*
        Message::Array(args)
    })
}
