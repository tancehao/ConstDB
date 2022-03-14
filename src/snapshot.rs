use std::io::{BufWriter, Write};
use tokio::io::AsyncReadExt;

use crate::{Bytes, CstError};
use crate::object::Object;

pub struct SnapshotWriter<W: Write> {
    io: BufWriter<W>,
    wrote_size: usize,
    checksum: u64,
}

impl<W: Write> SnapshotWriter<W> {
    pub fn new(size: usize, io: W) -> Self {
        SnapshotWriter{
            io: BufWriter::with_capacity(size, io),
            wrote_size: 0,
            checksum: 0,
        }
    }

    #[inline]
    pub fn write_integer(&mut self, i: i64) -> std::io::Result<&mut Self> {
        if i < 1<<6 {
            self.write_bytes([i as u8].as_ref())
        } else if i < 1<<14 {
            self.write_bytes(i16::to_be_bytes((i as i16) | 1<<14).as_ref())
        } else if i < 1<<30 {
            //self.write_bytes([(1<<7|i>>24) as u8, ((i>>16)&255) as u8, ((i>>8)&255) as u8, (i&255) as u8].as_ref())
            self.write_bytes(i32::to_be_bytes((i as i32) | 1<<31).as_ref())
        } else {
            self.write_bytes([3<<6 as u8].as_ref())?;
            self.write_bytes(i.to_be_bytes().as_ref())
        }
    }

    pub fn write_bytes(&mut self, src: &[u8]) -> std::io::Result<&mut Self> {
        // TODO need to update checksum
        self.checksum = 0;
        self.io.write(src).map(|x| {
            self.wrote_size += x;
            x
        })?;
        Ok(self)
    }

    pub fn write_entry(&mut self, key: &[u8], value: &Object) -> Result<(), CstError> {
        self.write_integer(key.len() as i64)?;
        self.write_bytes(key)?;
        value.save_snapshot(self)
    }

    pub fn write_byte(&mut self, d: u8) -> std::io::Result<&mut Self> {
        self.write_bytes([d;1].as_ref())
    }

    pub fn total_wrote(&self) -> usize {
        self.wrote_size
    }

    // TODO
    pub fn checksum(&self) -> u64 {
        self.checksum
    }

    pub fn flush(&mut self) -> Result<(), CstError> {
        self.io.flush().map_err(|x| x.into())
    }
}

pub type FileSnapshotLoader = SnapshotLoader<tokio::fs::File>;

#[derive(Debug)]
pub struct SnapshotLoader<T> {
    stat: SnapshotLoadProgress,
    io: T,
    read_size: usize,
}

impl<T> SnapshotLoader<T>
    where T: tokio::io::AsyncRead + std::marker::Unpin,
{
    pub fn new(f: T) -> Self {
        SnapshotLoader{
            stat: SnapshotLoadProgress::Begin,
            io: f,
            read_size: 0,
        }
    }

    pub async fn next(&mut self) -> Result<Option<SnapshotEntry>, CstError> {
        loop {
            match &mut self.stat {
                SnapshotLoadProgress::Begin => {
                    self.read_bytes(7).await?;
                    self.stat = SnapshotLoadProgress::Version;
                }
                SnapshotLoadProgress::Version => {
                    let version = self.read_bytes(4).await?;
                    if version.len() != 4 {
                        return Err(CstError::InvalidSnapshot(7));
                    }
                    let v = format!("{}.{}.{}.{}", version[0], version[1], version[2], version[3]).into();
                    self.stat = SnapshotLoadProgress::Node;
                    return Ok(Some(SnapshotEntry::Version(v)));
                }
                SnapshotLoadProgress::Node => {
                    let nodid = self.read_integer().await? as u64;
                    let alias_len = self.read_integer().await? as usize;
                    let alias = String::from_utf8(self.read_bytes(alias_len).await?).map_err(|_| CstError::InvalidSnapshot(self.read_size)).unwrap();
                    let addr_len = self.read_integer().await? as usize;
                    let addr = String::from_utf8(self.read_bytes(addr_len).await?).map_err(|_| CstError::InvalidSnapshot(self.read_size)).unwrap();
                    let uuid = self.read_integer().await? as u64;
                    self.convert_stat().await?;
                    return Ok(Some(SnapshotEntry::Node(nodid, alias, addr, uuid)));
                },
                SnapshotLoadProgress::Replicas(true) => {
                    let add_time = self.read_integer().await? as u64;
                    let nodid = self.read_integer().await? as u64;
                    let alias_len = self.read_integer().await? as usize;
                    let alias = String::from_utf8(self.read_bytes(alias_len).await?).map_err(|_| CstError::InvalidSnapshot(self.read_size)).unwrap();
                    let addr_len = self.read_integer().await? as usize;
                    let addr = String::from_utf8(self.read_bytes(addr_len).await?).map_err(|_| CstError::InvalidSnapshot(self.read_size)).unwrap();
                    let uuid = self.read_integer().await? as u64;
                    self.convert_stat().await?;
                    return Ok(Some(SnapshotEntry::ReplicaAdd(add_time, nodid, alias, addr, uuid)));
                },
                SnapshotLoadProgress::Replicas(false) => {
                    let addr_len = self.read_integer().await? as usize;
                    let addr = String::from_utf8(self.read_bytes(addr_len).await?).map_err(|_| CstError::InvalidSnapshot(self.read_size)).unwrap();
                    let t = self.read_integer().await? as u64;
                    self.convert_stat().await?;
                    return Ok(Some(SnapshotEntry::ReplicaDel(addr, t)));
                },
                SnapshotLoadProgress::Datas(size, current) => {
                    if *current < *size {
                        *current += 1;
                        let (key, value) = self.read_entry().await?;
                        return Ok(Some(SnapshotEntry::Data(key, value)));
                    } else {
                        self.convert_stat().await?;
                    }
                },
                SnapshotLoadProgress::Deletes(size, current) => {
                    if *current < *size {
                        *current += 1;
                        let (key, del_time) = self.read_key_int().await?;
                        return Ok(Some(SnapshotEntry::Deletes(key, del_time)));
                    } else {
                        self.convert_stat().await?;
                    }
                },
                SnapshotLoadProgress::Expires(size, current) => {
                    if *current < *size {
                        *current += 1;
                        let (key, ttl) = self.read_key_int().await?;
                        return Ok(Some(SnapshotEntry::Expires(key, ttl)));
                    } else {
                        self.convert_stat().await?;
                    }
                },
                SnapshotLoadProgress::Checksum => {
                    let _checksum = self.read_integer().await?;
                    // TODO need compare the checksum contained in snapshot with the one we calculated
                    self.stat = SnapshotLoadProgress::Finish;
                },
                SnapshotLoadProgress::Finish => {
                    return Ok(None);
                }
            }
        }
    }

    async fn convert_stat(&mut self) -> Result<(), CstError> {
        self.stat = match self.read_byte().await? {
            SNAPSHOT_FLAG_REPLICA_ADD => SnapshotLoadProgress::Replicas(true),
            SNAPSHOT_FLAG_REPLICA_REM => SnapshotLoadProgress::Replicas(false),
            SNAPSHOT_FLAG_DATAS => SnapshotLoadProgress::Datas(self.read_integer().await? as usize, 0),
            SNAPSHOT_FLAG_DELETES => SnapshotLoadProgress::Deletes(self.read_integer().await? as usize, 0),
            SNAPSHOT_FLAG_EXPIRES => SnapshotLoadProgress::Expires(self.read_integer().await? as usize, 0),
            SNAPSHOT_FLAG_CHECKSUM => SnapshotLoadProgress::Checksum,
            _ => {
                return Err(CstError::InvalidSnapshot(self.read_size));
            },
        };
        Ok(())
    }

    #[inline]
    pub async fn read_integer(&mut self) -> Result<i64, CstError> {
        let flag = self.read_byte().await?;
        match (flag >> 6) & 3 {
            0 => Ok(((flag<<2)>>2) as i64),
            1 => Ok(i16::from_be_bytes([flag & ((1<<6)-1), self.read_byte().await?]) as i64),
            2 => {
                let bytes = self.read_bytes(3).await?;
                Ok(i32::from_be_bytes([flag & ((1<<6)-1), bytes[0], bytes[1], bytes[2]]) as i64)
            }
            3 => {
                let bytes = self.read_bytes(8).await?;
                Ok(i64::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]]))
            }
            _ => unreachable!()
        }
    }

    pub async fn read_bytes(&mut self, size: usize) -> Result<Vec<u8>, CstError> {
        let mut datas = Vec::with_capacity(size);
        unsafe { datas.set_len(size); }
        self.io.read_exact(&mut datas).await?;
        Ok(datas)
    }

    pub async fn read_byte(&mut self) -> Result<u8, CstError> {
        self.read_bytes(1).await.map(|x| x[0])
    }

    pub async fn read_entry(&mut self) -> Result<(Bytes, Object), CstError> {
        let key = {
            let s = self.read_integer().await? as usize;
            self.read_bytes(s).await?.to_vec().into()
        };
        let v = Object::load_snapshot(self).await?;
        Ok((key, v))
    }

    async fn read_key_int(&mut self) -> Result<(Bytes, u64), CstError> {
        let key = {
            let s = self.read_integer().await? as usize;
            self.read_bytes(s).await?.to_vec().into()
        };
        Ok((key, self.read_integer().await? as u64))
    }

    #[inline]
    pub fn total_read(&self) -> usize {
        self.read_size
    }
}

#[derive(Clone, Debug)]
pub enum SnapshotEntry {
    Version(Bytes),
    Node(u64, String, String, u64), // node_id, node_alias, addr, uuid_he_sent
    ReplicaAdd(u64, u64, String, String, u64), // (add_time, node_id, node_alias, addr, uuid_he_sent)
    ReplicaDel(String, u64), // (addr, del_time)
    Data(Bytes, Object),
    Expires(Bytes, u64),
    Deletes(Bytes, u64),
}

pub const SNAPSHOT_FLAG_NODE: u8 = 2;
pub const SNAPSHOT_FLAG_REPLICA_ADD: u8 = 3;
pub const SNAPSHOT_FLAG_REPLICA_REM: u8 = 4;
pub const SNAPSHOT_FLAG_DATAS: u8 = 5;
pub const SNAPSHOT_FLAG_EXPIRES: u8 = 6;
pub const SNAPSHOT_FLAG_DELETES: u8 = 7;
pub const SNAPSHOT_FLAG_CHECKSUM: u8 = 8;

#[derive(Debug, Copy, Clone)]
enum SnapshotLoadProgress {
    Begin,
    Version,
    Node,
    Replicas(bool),
    Datas(usize, usize),
    Expires(usize, usize),
    Deletes(usize, usize),
    Checksum,
    Finish,
}

#[cfg(test)]
mod test {
    use bytes::buf::Writer;
    use bytes::BufMut;
    use tokio::fs::OpenOptions;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::macros::support::thread_rng_n;

    use crate::snapshot::{SnapshotLoader, SnapshotWriter};

    #[test]
    fn test_snapshot() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            test_snapshot_bytes().await;
        });
    }

    async fn test_snapshot_bytes() {
        {
            let f = std::fs::OpenOptions::new().create(true).read(true).write(true).truncate(true).open("test_spapshot_bytes").unwrap();
            let mut w = SnapshotWriter::new(2048, f);
            w.write_bytes(b"CONST");
            w.write_bytes(b"DB");
            w.write_integer(1);
            w.write_integer(2);
            w.write_integer(1<<13);
            w.write_integer(1<<20);
            w.write_integer(1<<26);
            w.write_integer(1<<30);
            w.write_integer(1<<31);
        }
        {
            let f = tokio::fs::OpenOptions::new().read(true).open("test_spapshot_bytes").await.unwrap();
            let mut r = SnapshotLoader::new(f);
            assert_eq!(r.read_bytes(5).await.unwrap(), b"CONST");
            assert_eq!(r.read_bytes(2).await.unwrap(), b"DB");
            assert_eq!(r.read_integer().await.unwrap(), 1);
            assert_eq!(r.read_integer().await.unwrap(), 2);
            assert_eq!(r.read_integer().await.unwrap(), 1<<13);
            assert_eq!(r.read_integer().await.unwrap(), 1<<20);
            assert_eq!(r.read_integer().await.unwrap(), 1<<26);
            assert_eq!(r.read_integer().await.unwrap(), 1<<30);
            assert_eq!(r.read_integer().await.unwrap(), 1<<31);
        }
    }
}