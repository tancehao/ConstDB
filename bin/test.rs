extern crate constdb;

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::result::Result::Ok;

use chrono::Local;
use clap::{App, Arg};
use env_logger;
use tokio::macros::support::thread_rng_n;
use tokio::net::TcpStream;

use constdb::conn::Conn;
use constdb::resp::Message;

pub fn main() {
    env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info");
    env_logger::builder()
        .format(|buf, r| {
            writeln!(
                buf,
                "{} {} [{}:{}] {}",
                Local::now().format("%Y-%m-%d %H:%M:%S.%6f"),
                r.level(),
                r.file().unwrap_or("<unnamed>"),
                r.line().unwrap_or(0),
                r.args(),
            )
        })
        .init();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local_set = tokio::task::LocalSet::new();
    local_set.block_on(&rt, async move {
        let replicas = parse_args();
        if replicas.len() != 3 {
            println!("there should be at least 3 nodes to test with");
        }
        test_active_active(replicas).await;
    });
}

fn parse_args() -> Vec<String> {
    let matches = App::new("ConstDB-cli")
        .version("1.1.0")
        .author("TanCehao tancehao93@163.com")
        .about("in-memory database supporting crdt")
        .arg(
            Arg::with_name("host")
                .short("h")
                .help("specify the server's host")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .help("specify the server's port")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("replicas")
                .short("rs")
                .long("replicas")
                .help("the addresses of the replicas to be test, should be of format ip:port")
                .takes_value(true)
                .multiple(true),
        )
        .get_matches();

    let nodes: Vec<String> = matches
        .values_of("replicas")
        .unwrap()
        .map(|x| x.to_string())
        .collect();
    nodes
}

async fn test_active_active(nodes: Vec<String>) {
    let mut replicas = Vec::with_capacity(nodes.len());
    for n in nodes {
        let conn = match TcpStream::connect(n.clone()).await {
            Ok(c) => c,
            Err(e) => {
                panic!("{}", e);
            }
        };
        let addr = conn.peer_addr().unwrap();
        replicas.push(Conn::new(Some(conn), addr.to_string()));
    }

    let mut r33 = replicas.pop().unwrap();
    let mut r22 = replicas.pop().unwrap();
    let mut r11 = replicas.pop().unwrap();
    let r1 = &mut r11;
    let r2 = &mut r22;
    let r3 = &mut r33;
    assert_eq!(exec!(r1, "INCR", "k1"), Message::Integer(1));

    assert_eq!(exec!(r2, "INCR", "k2"), Message::Integer(1));
    assert_eq!(exec!(r2, "INCR", "k2"), Message::Integer(2));
    assert_eq!(exec!(r2, "INCR", "k4"), Message::Integer(1));
    assert_eq!(exec!(r2, "INCR", "k4"), Message::Integer(2));
    assert_eq!(exec!(r2, "INCR", "k4"), Message::Integer(3));
    assert_eq!(exec!(r2, "INCR", "k4"), Message::Integer(4));

    // 2. send a command to one node, wait and check it in another node.
    assert_eq!(exec!(r1, "MEET", r2.addr.clone()), Message::Integer(1));
    sleep_mil!(3000);
    assert_eq!(exec!(r1, "GET", "k2"), Message::Integer(2));
    assert_eq!(exec!(r2, "GET", "k1"), Message::Integer(1));

    assert_eq!(exec!(r1, "INCR", "k3"), Message::Integer(1));
    sleep_mil!(20);
    assert_eq!(exec!(r2, "INCR", "k3"), Message::Integer(2));
    sleep_mil!(20);
    assert_eq!(exec!(r1, "GET", "k3"), Message::Integer(2));
    assert_eq!(exec!(r3, "MEET", r2.addr.clone()), Message::Integer(1));
    sleep_mil!(5000);
    assert_eq!(exec!(r3, "GET", "k3"), Message::Integer(2));
    assert_eq!(exec!(r3, "GET", "k4"), Message::Integer(4));
    assert_eq!(exec!(r3, "INCR", "k5"), Message::Integer(1));
    sleep_mil!(20);
    assert_eq!(exec!(r1, "INCR", "k5"), Message::Integer(2));
    sleep_mil!(20);
    assert_eq!(exec!(r2, "INCR", "k5"), Message::Integer(3));
    sleep_mil!(20);
    assert_eq!(exec!(r3, "GET", "k5"), Message::Integer(3));
    test_counters(r1, r2, r3).await;
    test_bytes(r1, r2, r3).await;
    test_set(r1, r2, r3).await;
    test_dict(r1, r2, r3).await;
}

async fn test_counters(r1: &mut Conn, r2: &mut Conn, r3: &mut Conn) {
    println!("");
    println!("----------------------------------------------------");
    println!("test INCR and DECR concurrently");
    println!("----------------------------------------------------");
    let key2 = "counter1";
    let mut clients = vec![&mut *r1, &mut *r2, &mut *r3];
    let mut v = 0;
    for _ in 0i32..1000 {
        let rand = thread_rng_n(10);
        let cmd = if rand % 2 == 0 {
            v += 1;
            "INCR"
        } else {
            v -= 1;
            "DECR"
        };
        let rand = thread_rng_n(1000) as usize;
        exec!(clients[rand % 3], cmd, key2);
        sleep_mil!(2);
    }
    sleep_mil!(200);
    assert_eq!(exec!(r1, "GET", key2), Message::Integer(v));
    assert_eq!(exec!(r2, "GET", key2), Message::Integer(v));
    assert_eq!(exec!(r3, "GET", key2), Message::Integer(v));

    println!("{}", green!("INCR AND DECR passed!"));
    test_counters_with_del(r1, r2, r3).await;
}

async fn test_counters_with_del(r1: &mut Conn, r2: &mut Conn, r3: &mut Conn) {
    println!("");
    // FIXME
    println!("----------------------------------------------------");
    println!("test INCR, DECR and DEL concurrently");
    println!("----------------------------------------------------");
    let key3 = "counter2";
    let mut clients = vec![&mut *r1, &mut *r2, &mut *r3];
    let mut cmds = vec![];
    for i in 0i32..100 {
        let rand = thread_rng_n(100);
        let cmd = match rand % 3 {
            0 => "INCR",
            1 => "DECR",
            2 => "DEL",
            _ => unreachable!(),
        };
        cmds.push(cmd);
        let rand = thread_rng_n(100) as usize;
        exec!(clients[rand % 3], cmd, key3);
        sleep_mil!(1);
        if i % 10 == 0 {
            sleep_mil!(200);
            let res1 = exec!(clients[0], "GET", key3);
            let res2 = exec!(clients[1], "GET", key3);
            let res3 = exec!(clients[2], "GET", key3);
            println!("res1: {}, res2: {}, res3: {}", res1, res2, res3);
            assert_eq!(res1, res2);
            assert_eq!(res1, res3);
        }
    }
    println!("{}", green!("INCR, DECR and DEL passed"));
}

async fn test_bytes(r1: &mut Conn, r2: &mut Conn, r3: &mut Conn) {
    println!("");
    println!("----------------------------------------------------");
    println!("test SET, DEL concurrently");
    println!("----------------------------------------------------");
    let mut d = HashSet::new();
    let mut clients = vec![&mut *r1, &mut *r2, &mut *r3];
    for _ in 0i32..1000 {
        let rand = thread_rng_n(100);
        let key = format!("key:{}", rand % 5);
        let rand_c = thread_rng_n(100) as usize;
        if rand % 3 == 0 {
            d.remove(&key);
            exec!(clients[rand_c % 3], "DEL", key);
        } else {
            let value = format!("value:{}", thread_rng_n(1000));
            d.insert(key.clone());
            exec!(clients[rand_c % 3], "SET", key, value);
        };
    }
    sleep_mil!(500);

    for key in d.iter() {
        let res1 = exec!(clients[0], "GET", key.clone());
        let res2 = exec!(clients[1], "GET", key.clone());
        let res3 = exec!(clients[2], "GET", key.clone());
        assert_eq!(res1, res2);
        assert_eq!(res1, res3);
    }
    println!("{}", green!("SET and DEL passed!"));
}

async fn test_set(r1: &mut Conn, r2: &mut Conn, r3: &mut Conn) {
    println!("");
    println!("----------------------------------------------------");
    println!("test SADD, SREM concurrently");
    println!("----------------------------------------------------");
    let key = "set1";
    let mut s = HashSet::new();
    let mut clients = vec![&mut *r1, &mut *r2, &mut *r3];
    for _ in 0i32..1000 {
        let rand = thread_rng_n(100);
        let member = format!("member:{}", rand);
        let cmd = if rand % 2 == 0 {
            s.insert(member.clone());
            "SADD"
        } else {
            s.remove(&member);
            "SREM"
        };
        let i = thread_rng_n(100) as usize;
        exec!(clients[i % 3], cmd, key, member);
        sleep_mil!(1);
    }
    sleep_mil!(200);
    let mut local_members: Vec<String> = s
        .into_iter()
        .map(|x| format!("{}", Message::BulkString(x.into())))
        .collect();
    local_members.sort();
    for c in clients {
        match exec!(c, "smembers", key) {
            Message::Array(members) => {
                let mut members: Vec<String> =
                    members.into_iter().map(|x| format!("{}", x)).collect();
                members.sort();
                assert_eq!(members, local_members);
            }
            _ => panic!("should be array"),
        }
    }

    println!("{}", green!("SADD and SREM passed"));

    println!("");
    println!("----------------------------------------------------");
    println!("test SADD, SREM and DEL concurrently");
    println!("----------------------------------------------------");
    let key = "set2";
    let mut s = HashSet::new();
    let mut clients = vec![&mut *r1, &mut *r2, &mut *r3];
    for _ in 0i32..1000 {
        let rand = thread_rng_n(20);
        let member = format!("member:{}", rand);
        let cmd = match rand % 9 {
            0 | 1 | 2 | 3 => {
                s.insert(member.clone());
                "SADD"
            }
            4 | 5 | 6 | 7 => {
                s.remove(member.as_str());
                "SREM"
            }
            8 => {
                s.clear();
                "DEL"
            }
            _ => unreachable!(),
        };
        let i = thread_rng_n(100) as usize;
        exec!(clients[i % 3], cmd, key, member);
        sleep_mil!(1);
        if i % 20 == 0 {
            sleep_mil!(50);
            let mut local_members: Vec<String> = s
                .clone()
                .into_iter()
                .map(|x| format!("{}", Message::BulkString(x.into())))
                .collect();
            local_members.sort();
            for c in clients.iter_mut() {
                match exec!(*c, "smembers", key) {
                    Message::Array(members) => {
                        let mut members: Vec<String> =
                            members.into_iter().map(|x| format!("{}", x)).collect();
                        members.sort();
                        assert_eq!(members, local_members);
                    }
                    _ => panic!("should be array"),
                }
            }
        }
    }

    println!("{}", green!("SADD, SREM and DEL passed"));
}

async fn test_dict(r1: &mut Conn, r2: &mut Conn, r3: &mut Conn) {
    println!("");
    println!("----------------------------------------------------");
    println!("test HSET and HDEL concurrently");
    println!("----------------------------------------------------");

    let key = "dict1";
    let mut d = HashMap::new();
    let mut clients = vec![&mut *r1, &mut *r2, &mut *r3];
    for _ in 0i32..100 {
        let rand = thread_rng_n(100);
        let field = format!("field:{}", thread_rng_n(10));
        let value = format!("value:{}", thread_rng_n(50));
        let rand_c = thread_rng_n(1000) as usize;
        match rand % 5 {
            1 | 2 | 3 | 4 => {
                d.insert(field.clone(), value.clone());
                exec!(clients[rand_c % 3], "HSET", key, field, value);
            }
            0 => {
                d.remove(field.as_str());
                exec!(clients[rand_c % 3], "HDEL", key, field);
            }
            _ => unreachable!(),
        };
        sleep_mil!(1);
    }
    sleep_mil!(200);
    let mut local_members: Vec<String> = d
        .into_iter()
        .map(|(k, v)| {
            format!(
                "{}",
                Message::Array(vec![
                    Message::BulkString(k.into()),
                    Message::BulkString(v.into())
                ])
            )
        })
        .collect();
    local_members.sort();
    for c in clients {
        match exec!(c, "HGETALL", key) {
            Message::Array(members) => {
                let mut members: Vec<String> =
                    members.into_iter().map(|x| format!("{}", x)).collect();
                members.sort();
                assert_eq!(members, local_members);
            }
            _ => panic!("should be array"),
        }
    }
    println!("{}", green!("HSET AND HDEL passed!"));

    println!("");
    println!("----------------------------------------------------");
    println!("test HSET, HDEL and DEL concurrently");
    println!("----------------------------------------------------");

    let key = "dict2";
    let mut m = HashMap::new();
    let mut clients = vec![&mut *r1, &mut *r2, &mut *r3];
    for i in 0i32..1000 {
        let rand = thread_rng_n(100);
        let field = format!("field:{}", thread_rng_n(10));
        let value = format!("value:{}", thread_rng_n(50));
        let rand_c = thread_rng_n(1000) as usize;
        match rand % 9 {
            0 | 1 | 2 | 3 => {
                m.insert(field.clone(), value.clone());
                exec!(clients[rand_c % 3], "HSET", key, field, value);
            }
            4 | 5 | 6 | 7 => {
                m.remove(field.as_str());
                exec!(clients[rand_c % 3], "HDEL", key, field);
            }
            8 => {
                m.clear();
                exec!(clients[rand_c % 3], "DEL", key);
            }
            _ => unreachable!(),
        };
        sleep_mil!(1);
        if i % 20 == 0 {
            sleep_mil!(50);
            let mut local_members: Vec<String> = m
                .clone()
                .into_iter()
                .map(|(k, v)| {
                    format!(
                        "{}",
                        Message::Array(vec![
                            Message::BulkString(k.into()),
                            Message::BulkString(v.into())
                        ])
                    )
                })
                .collect();
            local_members.sort();
            for c in clients.iter_mut() {
                match exec!(c, "HGETALL", key) {
                    Message::Array(members) => {
                        let mut members: Vec<String> =
                            members.into_iter().map(|x| format!("{}", x)).collect();
                        members.sort();
                        assert_eq!(members, local_members);
                    }
                    _ => panic!("should be array"),
                }
            }
        }
    }
    println!("{}", green!("HSET, HDEL and DEL passed!"));
}

#[macro_export]
macro_rules! exec {
    ($conn:expr, $($arg:expr),*) => {
    	{
			$conn.writable().await.unwrap();
			let mut args = vec![];
			$(
				args.push(Message::BulkString($arg.into()));
			)*
			send_and_receive($conn, Message::Array(args)).await
    	}
    };
}

async fn send_and_receive(c: &mut Conn, cmd: Message) -> Message {
    if let Err(e) = c.send_msg(cmd).await {
        panic!("{}", e);
    }
    match c.next_msg().await {
        Err(e) => panic!("{}", e),
        Ok(msg) => msg,
    }
}

#[macro_export]
macro_rules! green {
    ($e:expr) => {
        format!("\x1b[1;32m{}\x1b[0m", $e)
    };
}

#[macro_export]
macro_rules! sleep_mil {
    ($m:expr) => {
        tokio::time::sleep(tokio::time::Duration::from_millis($m as u64)).await;
    };
}
