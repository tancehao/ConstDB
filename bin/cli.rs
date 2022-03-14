use std::collections::VecDeque;
use std::io::{stdin, stdout, Write};
use std::result::Result::Ok;

use clap::{App, Arg};
use tokio::net::TcpStream;

use constdb::conn::Conn;
use constdb::CstError;
use constdb::resp::Message;

pub fn main() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let local_set = tokio::task::LocalSet::new();
    local_set.block_on(&rt, async move {
        let addr = parse_args();
        loop {
            let conn = TcpStream::connect(addr.clone()).await.expect("cannot connect to server");
            let server_addr = conn.peer_addr().expect("unable to fetch peer_addr");
            let conn = Conn::new(Some(conn), server_addr.to_string());
            if let Err(e) = interact(conn).await {
                println!("err: {}", e);
                break;
            }
        }
    });
}

async fn interact(mut conn: Conn) -> Result<(), CstError> {
    let input = stdin();
    let mut output = stdout();

    let mut cmd = String::new();
    let mut cmd_history = VecDeque::with_capacity(1024);
    let si = format!("{}> ", conn.addr);
    loop {
        output.write(si.as_bytes()).unwrap();
        output.flush()?;
        cmd.clear();
        let size = input.read_line(&mut cmd)?;
        cmd.truncate(size - 1);
        if cmd.len() == 0 {
            continue;
        }
        let args: Vec<Message> = cmd.split(' ').into_iter().filter(|&x| x.len() > 0).map(|x| Message::BulkString(x.into())).collect();
        match args.first() {
            None => continue,
            Some(Message::BulkString(s)) => {
                let cmd_name = String::from(s.clone()).to_ascii_lowercase();
                if cmd_name == "exit" {
                    std::process::exit(0);
                }
            }
            _ => unreachable!(),
        }

        cmd_history.push_back(args.clone());
        if cmd_history.len() > 1024 {
            cmd_history.pop_front();
        }

        conn.send_msg(Message::Array(args)).await?;
        let resp = conn.next_msg().await?;
        format_msg(&mut output, &resp)?;
        output.flush()?;
    }
}

fn format_msg<T: Write>(w: &mut T, msg: &Message) -> Result<(), std::io::Error> {
    match msg {
        Message::None => {},
        Message::Nil => writeln!(w, "(nil)")?,
        Message::Array(args) => {
            for arg in args {
                format_msg(w, arg)?;
            }
        }
        Message::BulkString(s) => writeln!(w, "\"{}\"", String::from(s.clone()))?,
        Message::Integer(i) => writeln!(w, "(int) {}", i)?,
        Message::Error(e) => writeln!(w, "(error) {}", String::from(e.clone()))?,
        Message::String(s) => writeln!(w, "+{}", String::from(s.clone()))?,
    }
    Ok(())
}

fn parse_args() -> String {
    let matches = App::new("ConstDB-cli")
        .version("1.1.0")
        .author("TanCehao tancehao93@163.com")
        .about("in-memory database supporting crdt")
        .arg(Arg::with_name("host")
            .short("h")
            .help("specify the server's host")
            .takes_value(true))
        .arg(Arg::with_name("port")
            .short("p")
            .help("specify the server's port")
            .takes_value(true))
        .get_matches();

    let ip = matches.value_of("host").unwrap_or("127.0.0.1");
    let port = matches.value_of("port").unwrap_or("9001");
    format!("{}:{}", ip, port)
}