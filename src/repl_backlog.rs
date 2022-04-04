use crate::resp::Message;
use crate::server::{new_events_chann, EventsConsumer, EventsProducer};
use std::collections::VecDeque;

#[derive(Debug)]
pub struct ReplBacklog {
    repl_backlog: VecDeque<(u64, &'static str, Vec<Message>)>,
    latest_repl_uuid_overflowed: Option<u64>,
    repl_log_size: u64,
    repl_log_size_limit: u64,
    node_id: u64,
    events: (EventsProducer<u64>, EventsConsumer<u64>),
}

impl ReplBacklog {
    pub fn new(node_id: u64, limit: u64) -> Self {
        Self {
            repl_backlog: VecDeque::with_capacity(128),
            latest_repl_uuid_overflowed: None,
            repl_log_size: 0,
            repl_log_size_limit: limit,
            node_id,
            events: new_events_chann(),
        }
    }

    pub fn new_watcher(&self) -> EventsConsumer<u64> {
        self.events.0.new_consumer()
    }

    pub fn set_size_limit(&mut self, limit: u64) {
        self.repl_log_size_limit = limit;
        self.evict_earliest();
    }

    pub fn replicate_cmd(&mut self, uuid: u64, cmd_name: &'static str, args: Vec<Message>) {
        let s: usize = args.iter().map(|x| x.size()).sum();
        self.repl_backlog.push_back((uuid, cmd_name, args));
        debug!(
            "Wrote a new command into the ReplBacklog, cmd_name={}, uuid={}",
            cmd_name, uuid
        );
        self.repl_log_size += s as u64 + 16;
        self.evict_earliest();
        self.events.0.trigger(uuid);
    }

    fn evict_earliest(&mut self) {
        while self.repl_log_size > self.repl_log_size_limit {
            match self.repl_backlog.pop_front() {
                None => {
                    error!("the repl_log is empty while repl_log_size is greater than 0");
                }
                Some((u, _, ms)) => {
                    let s: usize = ms.iter().map(|x| x.size()).sum();
                    self.repl_log_size -= s as u64;
                    self.latest_repl_uuid_overflowed = Some(u);
                }
            }
        }
        self.repl_backlog.shrink_to(self.repl_backlog.len());
    }

    // get the following log after uuid
    pub fn log_following(&self, uuid: u64) -> Option<(u64, Message)> {
        let msg_pos = if uuid == 0 {
            if self.latest_repl_uuid_overflowed.is_some() {
                None
            } else {
                Some(0)
            }
        } else {
            self.uuid_index(uuid).map(|x| x + 1)
        };
        match msg_pos {
            None => return None,
            Some(pos) => match self.repl_backlog.get(pos) {
                None => None,
                Some((next_uuid, cmd_name, args)) => {
                    let mut replicates = Vec::with_capacity(args.len() + 5);
                    replicates.push(Message::BulkString("replicate".into()));
                    replicates.push(Message::Integer(self.node_id as i64));
                    replicates.push(Message::Integer(uuid as i64));
                    replicates.push(Message::Integer(*next_uuid as i64));
                    replicates.push(Message::BulkString((*cmd_name).into()));
                    replicates.extend_from_slice(args);
                    Some((*next_uuid, Message::Array(replicates)))
                }
            },
        }
    }

    fn uuid_index(&self, uuid: u64) -> Option<usize> {
        let l = self.repl_backlog.len();
        if l == 0 || self.repl_backlog[0].0 > uuid {
            return None;
        }
        if uuid > self.repl_backlog[l - 1].0 {
            return None;
        }
        let (mut start, mut end) = (0usize, l - 1);
        loop {
            if end - start == 1 {
                if self.repl_backlog[start].0 == uuid {
                    return Some(start);
                }
                if self.repl_backlog[end].0 == uuid {
                    return Some(end);
                }
                return None;
            }
            let middle = (start + end) / 2;
            let uuid_at_middle = self.repl_backlog[middle].0;
            if uuid_at_middle != uuid && middle == start {
                return None;
            }
            if uuid_at_middle > uuid {
                end = middle;
            } else if uuid_at_middle < uuid {
                start = middle;
            } else {
                return Some(middle);
            }
        }
    }

    pub fn log_at(&self, uuid: u64) -> Option<Message> {
        match self.uuid_index(uuid) {
            None => None,
            Some(idx) => match self.repl_backlog.get(idx) {
                None => None,
                Some((_, cmd_name, args)) => {
                    let mut cmd = Vec::with_capacity(args.len() + 1);
                    cmd.push(Message::BulkString(cmd_name.to_string().into()));
                    cmd.extend_from_slice(args);
                    Some(Message::Array(cmd))
                }
            },
        }
    }

    pub fn log_uuids(&self) -> Vec<u64> {
        self.repl_backlog.iter().map(|(x, _, _)| *x).collect()
    }

    #[inline]
    pub fn first_uuid(&self) -> u64 {
        self.repl_backlog
            .front()
            .map(|(u, _, _)| *u)
            .unwrap_or_default()
    }

    #[inline]
    pub fn last_uuid(&self) -> u64 {
        self.repl_backlog
            .back()
            .map(|(u, _, _)| *u)
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod test {
    use crate::cmd::NextArg;
    use crate::repl_backlog::ReplBacklog;
    use crate::resp::Message;
    use tokio::macros::support::thread_rng_n;

    #[test]
    fn test_replog() {
        let random_bytes = |size: usize| -> Vec<u8> {
            (0..size)
                .map(|_| thread_rng_n(26) + 48)
                .map(|x| x as u8)
                .collect()
        };

        let mut log = ReplBacklog::new(1, 1024 * 1024 * 100);
        let mut uuids = Vec::with_capacity(50);
        let mut uuid = 1u64;
        for _ in 0..50 {
            let args_len = thread_rng_n(5);
            let args: Vec<Message> = (0..args_len)
                .map(|_| random_bytes(thread_rng_n(6) as usize))
                .map(|x| Message::BulkString(x.into()))
                .collect();
            uuids.push((uuid, args));
            uuid += thread_rng_n(5) as u64 + 1;
        }
        for (uuid, args) in &uuids {
            log.replicate_cmd(*uuid, "TestCommand", args.clone());
        }
        assert_eq!(log.first_uuid(), uuids[0].0);
        assert_eq!(log.last_uuid(), uuids[49].0);
        for _ in 0..50 {
            let idx = thread_rng_n(49) as usize;
            let (uuid, args) = uuids.get(idx).unwrap();
            assert_eq!(log.uuid_index(*uuid), Some(idx));
            let next = log.log_following(*uuid);
            assert!(next.is_some());
            let (next_uuid, args) = next.unwrap();
            let mut args = match args {
                Message::Array(args) => args.into_iter(),
                _ => panic!("should be array"),
            };
            assert_eq!(uuids[idx + 1].0, next_uuid);
            assert_eq!(args.next_string().unwrap(), "replicate");
            assert_eq!(args.next_u64().unwrap(), 1);
            assert_eq!(args.next_u64().unwrap(), *uuid);
            assert_eq!(args.next_u64().unwrap(), next_uuid);
            assert_eq!(args.next_string().unwrap(), "TestCommand");
            for arg in uuids[idx + 1].1.iter() {
                match arg {
                    Message::BulkString(arg) => assert_eq!(&(args.next_bytes().unwrap()), arg),
                    _ => panic!("should be bulk string"),
                }
            }
        }
    }
}
