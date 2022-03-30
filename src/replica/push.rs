use crate::conf::GLOBAL_CONF;
use crate::conn::writer::Writer;
use crate::replica::replica::ReplicaMeta;
use crate::resp::Message;
use crate::server::{EventsConsumer, Server};
use crate::{now_secs, CstError};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;
use tokio::fs::OpenOptions;
use tokio::time::sleep;
use tokio::time::Duration;

#[derive(Debug)]
pub struct Pusher {
    pub(crate) uuid_i_sent: u64,
    pub(crate) uuid_i_acked: u64,
    pub(crate) latest_ack_time: u64,
    pub(crate) meta: ReplicaMeta,
    pub(crate) stats: PushStat,
    pub(crate) writer: Writer,
    pub(crate) events: EventsConsumer<u64>,
}

#[derive(Debug, Clone)]
pub enum PushStat {
    SyncReceived,
    WaitingDump(Option<Pid>, String, u64),
    SendingSnapshot(String, u64),
    PushingCommands,
}

impl Pusher {
    // send out snapshot to the replica.
    // this function runs in io threads, not in the main one.
    pub async fn sending_snapshot(&mut self) -> Result<(), CstError> {
        loop {
            match &mut self.stats {
                PushStat::SyncReceived => {
                    return Ok(());
                }
                PushStat::WaitingDump(pid, file_name, tombstone) => {
                    debug!(
                        "I am at WaitingDump stat in the view of replica at {}",
                        self.meta.he.addr
                    );
                    if let Some(pid) = pid.take() {
                        loop {
                            sleep(Duration::from_millis(500)).await;
                            match waitpid(pid, Some(WaitPidFlag::WNOHANG)) {
                                Ok(WaitStatus::StillAlive) => continue,
                                Ok(_) => break,
                                Err(e) => {
                                    error!("Error occurred when waiting for child process, {}", e);
                                    return Err(CstError::SystemError);
                                }
                            }
                        }
                        debug!("Child process has finished dumping the snapshot");
                    }
                    self.stats = PushStat::SendingSnapshot(file_name.clone(), *tombstone);
                }
                PushStat::SendingSnapshot(filename, uuid_tombstone) => {
                    debug!("Child process finished dumping the snapshot");
                    let mut snapshot = OpenOptions::new().read(true).open(filename).await?;
                    let snapshot_size = snapshot.metadata().await?.len();
                    self.writer
                        .send_msg(Message::Integer(snapshot_size as i64))
                        .await?;
                    self.writer.send_file(&mut snapshot).await?;
                    self.uuid_i_sent = *uuid_tombstone;
                    debug!(
                        "Finished sending the snapshot to replica at {}, before uuid={}",
                        self.meta.he.addr, *uuid_tombstone
                    );
                    self.stats = PushStat::PushingCommands;
                }
                PushStat::PushingCommands => return Ok(()),
            }
        }
    }

    // waiting for an EVENT_TYPE_REPLICATED event. If it comes, we know there
    // are some new commands that ought to be replicated.
    pub async fn watching_my_replog(&mut self) -> Result<(), CstError> {
        self.writer.flush().await?;
        match &mut self.stats {
            PushStat::PushingCommands => {}
            _ => return Ok(()),
        }
        tokio::select! {
            _ = self.events.occured(&self.uuid_i_sent) => {},
            _ = sleep(Duration::from_secs(4)) => {},
        }
        Ok(())
    }

    // start to dump a snapshot or send our new replicates to the replica.
    // this function runs in the main thread.
    pub fn push_to_replica_in_main(
        &mut self,
        server: &mut Server,
        uuid_he_sent: u64,
    ) -> Result<(), CstError> {
        match self.stats {
            PushStat::SyncReceived => {
                debug!("Replica at {} is in SyncReceived stat", self.meta.he.addr);
                server.replicas.update_replica_identity(&self.meta.he);
                if self.meta.uuid_i_sent > 0
                    && server.repl_backlog.first_uuid() < self.meta.uuid_i_sent
                {
                    self.writer.write_msg(Message::Integer(0));
                    self.stats = PushStat::PushingCommands;
                } else {
                    match server.dump_snapshot_in_background() {
                        Err(e) => {
                            error!("Failed to dump the snapshot for {}", e);
                            return Err(CstError::SystemError);
                        }
                        Ok((pid, file_name, tombstone)) => {
                            debug!("Forked a child process to dump the snapshot, pid={:?}", pid);
                            self.stats = PushStat::WaitingDump(pid, file_name, tombstone);
                        }
                    }
                }
                server.replicas.update_replica_identity(&self.meta.he);
            }
            PushStat::PushingCommands => {
                let mut sent = 0;
                for _ in 0..16 {
                    let r = self.send_my_replicates(server);
                    match r {
                        Ok(true) => sent += 1,
                        Ok(false) => break,
                        Err(_) => {
                            error!("the replica {} is too delayed", self.meta.he.addr);
                            // TODO
                            break;
                        }
                    }
                }
                debug!(
                    "Sent {} commands to the replica at {}",
                    sent, self.meta.he.addr
                );
                let now = now_secs();

                if self.latest_ack_time + (GLOBAL_CONF.replica_heartbeat_frequency as u64) < now {
                    self.writer
                        .write_msg(mkcmd!("REPLACK", uuid_he_sent, server.next_uuid(false)));
                    self.latest_ack_time = now;
                }
            }
            _ => {}
        }
        Ok(())
    }

    // TODO, need to tell the caller whether it's uuid is too late
    fn send_my_replicates(&mut self, server: &mut Server) -> Result<bool, CstError> {
        match server.repl_backlog.log_following(self.uuid_i_sent) {
            None => Ok(false),
            Some((uuid, msg)) => {
                debug!(
                    "Sending my replicate with uuid={} to the replica at {}",
                    uuid, self.meta.he.addr
                );
                self.writer.write_msg(msg);
                self.uuid_i_sent = uuid;
                server
                    .replicas
                    .update_replica_push_stat(&self.meta.he, uuid, self.uuid_i_acked);
                Ok(true)
            }
        }
    }
}
