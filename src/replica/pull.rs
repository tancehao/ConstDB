use std::collections::VecDeque;

use tokio::io::{AsyncSeekExt, SeekFrom};

use crate::cmd::{Cmd, NextArg};
use crate::conn::reader::Reader;
use crate::link::SharedLink;
use crate::replica::replica::{Replica, ReplicaMeta};
use crate::resp::Message;
use crate::server::Server;
use crate::snapshot::{FileSnapshotLoader, SnapshotEntry, SnapshotLoader};
use crate::CstError;

#[derive(Debug)]
pub struct Puller {
    pub(crate) uuid_he_sent: u64,
    pub(crate) uuid_he_acked: u64,
    pub(crate) meta: ReplicaMeta,

    pub(crate) stats: PullStat,
    pub(crate) reader: Reader,
    pub(crate) snapshot_entries: VecDeque<SnapshotEntry>,
    pub(crate) replicates: VecDeque<Message>,
}

#[derive(Debug)]
pub enum PullStat {
    SyncSent,
    DownloadingSnapshot(usize),
    LoadingSnapshot(FileSnapshotLoader),
    PullingCommands,
}

impl Puller {
    pub async fn download_snapshot(&mut self) -> Result<(), CstError> {
        loop {
            match &mut self.stats {
                PullStat::SyncSent => {
                    debug!("Replica at {} is in SyncSent stat", self.meta.he.addr);
                    match self.reader.next_msg().await? {
                        Message::Integer(snapshot_size) => {
                            self.stats = if snapshot_size > 0 {
                                PullStat::DownloadingSnapshot(snapshot_size as usize)
                            } else {
                                PullStat::PullingCommands
                            };
                            debug!(
                                "Received response of SYNC cmd from {}, snapshot_size={}",
                                self.meta.he.addr, snapshot_size
                            );
                        }
                        _ => return Err(CstError::InvalidType), // TODO
                    }
                }
                PullStat::DownloadingSnapshot(snapshot_size) => {
                    debug!(
                        "Replica at {} is in DownloadingSnapshot stat",
                        self.meta.he.addr
                    );
                    let file_name = format!("snapshot.{}", self.meta.he.addr);
                    let mut snapshot = tokio::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .read(true)
                        .truncate(true)
                        .open(file_name)
                        .await?;
                    if let Err(e) = self
                        .reader
                        .save_to_file(&mut snapshot, *snapshot_size)
                        .await
                    {
                        error!("Failed to save snapshot into local file because {}", e);
                        return Err(CstError::SystemError);
                    }
                    debug!(
                        "Finished downloading the snapshot from replica at {}, snapshot_size={}",
                        self.meta.he.addr, *snapshot_size
                    );
                    snapshot.seek(SeekFrom::Start(0)).await?;
                    self.stats = PullStat::LoadingSnapshot(SnapshotLoader::new(snapshot));
                }
                PullStat::LoadingSnapshot(loader) => {
                    debug!(
                        "Replica at {} is in LoadingSnapshot stat",
                        self.meta.he.addr
                    );
                    for _ in 0usize..32 {
                        match loader.next().await? {
                            Some(entry) => self.snapshot_entries.push_back(entry),
                            None => {
                                debug!(
                                    "Finished load snapshot from replica at {}",
                                    self.meta.he.addr
                                );
                                break;
                            }
                        }
                    }
                    if self.snapshot_entries.len() == 0 {
                        self.stats = PullStat::PullingCommands;
                    }
                    return Ok(());
                }
                PullStat::PullingCommands => return Ok(()),
            }
        }
    }

    pub async fn accept_replicates(&mut self) -> Result<(), CstError> {
        loop {
            match &mut self.stats {
                PullStat::PullingCommands => {
                    debug!(
                        "Replica at {} is in PullingCommands stat",
                        self.meta.he.addr
                    );
                    for _ in 0u8..16 {
                        match self.reader.try_next_msg() {
                            Err(_) => {
                                return Err(CstError::ConnBroken(self.meta.he.addr.clone()));
                            }
                            Ok(None) => break,
                            Ok(Some(msg)) => self.replicates.push_back(msg),
                        }
                    }
                    if self.replicates.is_empty() {
                        self.reader.readable().await?;
                        if let Some(0) = self.reader.read_input()? {
                            return Err(CstError::ConnBroken(self.meta.he.addr.clone()));
                        }
                    } else {
                        return Ok(());
                    }
                }
                _ => return Ok(()),
            }
        }
    }

    pub fn merge_replicates_in_main(&mut self, server: &mut Server) -> Result<(), CstError> {
        match &mut self.stats {
            PullStat::LoadingSnapshot(_) => {
                debug!(
                    "Replica at {} is in LoadingSnapshot stat",
                    self.meta.he.addr
                );
                while let Some(entry) = self.snapshot_entries.pop_front() {
                    match entry {
                        SnapshotEntry::Version(version) => {
                            info!("Received snapshot with version {:?}", version);
                        }
                        SnapshotEntry::Data(k, v) => {
                            debug!("Merging entry from snapshot, k={:?}, v={:?}", k, v);
                            server.db.merge_entry(k, v);
                        }
                        SnapshotEntry::Deletes(k, uuid) => server.db.delete(&k, uuid),
                        SnapshotEntry::Expires(k, t) => server.db.expire_at(&k, t),
                        SnapshotEntry::Node(node_id, node_alias, _addr, uuid) => {
                            self.uuid_he_sent = uuid;
                            self.meta.he.id = node_id;
                            self.meta.he.alias = node_alias;
                        }
                        SnapshotEntry::ReplicaAdd(add_time, node_id, node_alias, addr, uuid) => {
                            if node_id == self.meta.myself.id {
                                continue;
                            }
                            debug!("Found a new replica from the snapshot, node_id={}, alias={}, addr={}, uuid={}", node_id, node_alias, addr, uuid);
                            let mut r = Replica::new(
                                addr.clone(),
                                server.node_id,
                                server.config.node_alias.clone(),
                                format!("{}:{}", server.config.ip, server.config.port),
                            );
                            r.meta.he.id = node_id;
                            r.meta.uuid_he_sent = uuid;
                            r.meta.he.alias = node_alias;
                            r.events = Some(server.repl_backlog.new_watcher(0));
                            if server
                                .replicas
                                .add_replica(addr.clone(), r.meta.clone(), add_time)
                            {
                                let mut sl = SharedLink::from(r);
                                tokio::spawn(async move {
                                    sl.prepare().await;
                                });
                            }
                        }
                        SnapshotEntry::ReplicaDel(addr, t) => {
                            server.replicas.remove_replica(&addr, t);
                        }
                    }
                }
            }
            PullStat::PullingCommands => {
                let mut applied = 0;
                for _ in 0..16 {
                    while let Some(cmd) = self.replicates.pop_front() {
                        match self.apply_his_replicates(server, cmd) {
                            Ok(true) => applied += 1,
                            Ok(false) => break,
                            Err(_) => {
                                // some commands are lost, resync from the latest position
                                error!(
                                    "some commands from the peer {} are lost",
                                    self.meta.he.alias
                                );
                                // TODO
                                break;
                            }
                        }
                    }
                }

                debug!(
                    "Succeed to exchange commands with replica at {}, applied={}",
                    self.meta.he.addr, applied
                );
            }
            _ => {}
        }
        server.replicas.update_replica_pull_stat(
            &self.meta.he,
            self.uuid_he_sent,
            self.uuid_he_acked,
        );
        Ok(())
    }

    fn apply_his_replicates(
        &mut self,
        server: &mut Server,
        cmd: Message,
    ) -> Result<bool, CstError> {
        let mut args = match cmd {
            Message::Array(args) => {
                debug!(
                    "Trying to apply the replicates from {}, args: {:?}",
                    self.meta.he.addr, args
                );
                args.into_iter()
            }
            _ => return Err(CstError::InvalidRequestMsg("should be array".into())),
        };

        let replicate_command = args.next_bytes()?;
        let cmd_name = replicate_command.as_bytes().to_ascii_lowercase();
        match cmd_name.as_slice() {
            b"replicate" => {
                let nodeid = args.next_u64()?;
                let last_uuid = args.next_u64()?;
                debug!("Comparing uuids when apply his replicas, nodeid={}, last_uuid={}, self.meta.uuid_he_sent={}", nodeid, last_uuid, self.uuid_he_sent);
                if self.uuid_he_sent < last_uuid {
                    debug!(
                        "uuid_he_sent: {}, last_uuid: {}",
                        self.uuid_he_sent, last_uuid
                    );
                    return Err(CstError::ReplicateCommandsLost(self.meta.he.addr.clone()));
                } else if self.uuid_he_sent > last_uuid {
                    // duplicated commands
                    return Ok(false);
                } else {
                    let current_uuid = args.next_u64()?;
                    let rpl_command_name = args.next_bytes()?;
                    let args: Vec<Message> = args.collect();
                    match Cmd::new(rpl_command_name.as_bytes(), args) {
                        Err(e) => {
                            error!(
                                "the replica named {} sent us an unknown command {}",
                                self.meta.he.alias, e
                            );
                            self.uuid_he_sent = current_uuid;
                            // we treat it not as a bug because the replica may have a greater version.
                        }
                        Ok(cmd) => {
                            // Note! We do not replicate those commands received from our replicas.
                            if let Err(e) =
                                cmd.exec_detail(server, None, nodeid, current_uuid, false)
                            {
                                error!("error '{}' occurred when executing command from our replica {}", e, self.meta.he.alias);
                            }
                            self.uuid_he_sent = current_uuid;
                        }
                    };
                }
            }
            b"replack" => {
                self.uuid_he_acked = args.next_u64()?;
            }
            _ => {
                error!("we met an invalid command {:?} from our replica {} which should be `replicate` or `replack`", cmd_name, self.meta.he.addr);
                return Err(CstError::InvalidRequestMsg(format!(
                    "{:?}",
                    cmd_name.to_vec()
                )));
            }
        }
        Ok(true)
    }
}
