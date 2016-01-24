use std::io::Write;

#[macro_use]
mod zfs_error;
use zfs_error::ZfsError;

mod zfs;
use zfs::ZFS;

mod inheritable_pipe;
use inheritable_pipe::InheritablePipe;

pub struct ZSnapMgr {
    zfs: ZFS,
}

impl ZSnapMgr {
    pub fn new(use_sudo: bool) -> ZSnapMgr {
        ZSnapMgr { zfs: ZFS { use_sudo: use_sudo } }
    }

    pub fn get_volumes(&self) -> Result<Vec<String>, ZfsError> {
        self.zfs.volumes(None)
    }

    pub fn get_snapshots(&self, dataset: Option<&str>) -> Result<Vec<String>, ZfsError> {
        self.zfs.snapshots(dataset)
    }

    pub fn backup(&self,
                  path: &str,
                  snapshot: &str,
                  passphrase: &str,
                  incremental_start: Option<&str>)
                  -> Result<(), ZfsError> {
        let mut passphrase_pipe =
            zfstry!(InheritablePipe::new(), or "failed to create passphrase pipe");

        zfstry!(write!(passphrase_pipe, "{}\n", passphrase), or "failed to write passphrase to pipe");

        self.zfs.send(snapshot,
                      &format!("{}/{}.zfs.bz2.gpg", path, snapshot.replace("/", "_")),
                      incremental_start,
                      Some(&format!("pbzip2 | gpg --batch --symmetric --passphrase-fd {} \
                                     --output -",
                                    passphrase_pipe.child_fd())))
    }
}
