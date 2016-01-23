#![cfg_attr(test, allow(dead_code))]

use std::collections::btree_map::{BTreeMap, Entry, IterMut};
use std::env;
use std::iter::Iterator;
use std::process;

extern crate zsnapmgr;
use zsnapmgr::ZSnapMgr;

mod enumerate_files_no_error;
use enumerate_files_no_error::EnumerateFilesNoError;

mod table;
use table::Table;

static USE_SUDO: bool = true;

#[derive(Debug)]
struct Backup {
    filename_base: String,
    volume: String,
    start_snapshot: Option<String>,
    end_snapshot: Option<String>,
}

struct Backups {
    backups_by_volume: BTreeMap<String, Backup>
}

struct BackupsIterMut<'a> {
    iter_mut: IterMut<'a, String, Backup>,
}

impl<'a> Iterator for BackupsIterMut<'a> {
    type Item = &'a mut Backup;
    fn next(&mut self) -> Option<&'a mut Backup> {
        match self.iter_mut.next() {
            Some((_, backup)) => Some(backup),
            None => None,
        }
    }
}

impl Backups {
    pub fn new() -> Backups {
        Backups {
            backups_by_volume: BTreeMap::new()
        }
    }

    pub fn insert(&mut self, filename_base: String, volume: String, start_snapshot: Option<String>) {

        match self.backups_by_volume.entry(volume.clone()) {
            Entry::Occupied(ref mut entry) => {
                let backup = entry.get_mut();
                if start_snapshot.is_some()
                    && (
                        backup.start_snapshot.is_none()
                        || start_snapshot.as_ref().unwrap() > backup.start_snapshot.as_ref().unwrap()
                    ) {
                    backup.start_snapshot = start_snapshot;
                    backup.filename_base = filename_base;
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(Backup {
                    filename_base: filename_base,
                    volume: volume,
                    start_snapshot: start_snapshot,
                    end_snapshot: None,
                });
            },
        }
    }

    pub fn values(self) -> Vec<Backup> {
        let mut vec: Vec<Backup> = Vec::new();
        for (_k, v) in self.backups_by_volume.into_iter() {
            if v.end_snapshot.is_some() {
                vec.push(v);
            }
        }
        vec
    }

    fn iter_mut<'a>(&'a mut self) -> BackupsIterMut<'a> {
        BackupsIterMut {
            iter_mut: self.backups_by_volume.iter_mut()
        }
    }
}

fn gather_volumes(path: &str) -> Vec<Backup> {
    let z = ZSnapMgr::new(USE_SUDO);
    let snapshots = match z.get_snapshots(None) {
        Ok(s) => s,
        Err(e) => {
            println!("Error getting snapshots from ZFS: {}", e);
            return vec!();
        }
    };

    let volumes: Vec<String> = match z.get_volumes() {
        Ok(v) => v,
        Err(e) => {
            println!("Error getting volumes from ZFS: {}", e);
            return vec!();
        }
    };

    let mut backups = Backups::new();

    match EnumerateFilesNoError::new(path) {
        Ok(enumerator) => {
            for filename in enumerator {
                if let Some(zfs_pos) = filename.find(".zfs") {
                    if !(&filename).ends_with("_partial") {
                        let parts = filename[0..zfs_pos].splitn(2, '@').collect::<Vec<&str>>();
                        let volume_name = parts[0].replace("_", "/");
                        let backup_snap = parts[1];

                        if volumes.binary_search(&volume_name).is_ok() {
                            backups.insert(volume_name.to_string(), volume_name.to_string(), Some(backup_snap.to_string()));
                        }
                        else {
                            let volume_name_mod = "/".to_string() + &volume_name;
                            let matches = volumes.iter().filter(|ref vol| {
                                vol.ends_with(&volume_name_mod)
                            }).collect::<Vec<&String>>();

                            if matches.len() == 1 {
                                backups.insert(matches[0].clone(), volume_name.to_string(), Some(backup_snap.to_string()));
                            }
                            else {
                                print!("Backup filename \"{}\" ", filename);
                                if matches.len() > 1 {
                                    println!("matches more than one volume.\nIt could be any of: {:?}", matches);
                                }
                                else {
                                    println!("doesn't match any volumes.");
                                }
                                println!("Skipping it.\n");
                            }
                        }
                    }
                }
            }
        },
        Err(e) => {
            println!("Error listing directory \"{}\": {}", path, e);
            return vec!();
        },
    }

    // Now fill in the latest snapshot available for each volume in the proposed backups.
    for backup in backups.iter_mut() {
        let volume_at = backup.volume.clone() + "@";
        let volume_snaps = snapshots.iter().filter(|snap| {
            snap.starts_with(&volume_at)
        }).collect::<Vec<&String>>();


        if backup.start_snapshot.is_some() {
            // Check that the start snapshot still exists.
            let start_snapshot = volume_at.clone() + backup.start_snapshot.as_ref().unwrap();

            if !volume_snaps.binary_search(&&start_snapshot).is_ok() {
                println!("Snapshot \"{}\" doesn't exist any more; doing full backup instead.\n", start_snapshot);
                backup.start_snapshot = None;
            }
        }

        if volume_snaps.len() == 1 && backup.start_snapshot.is_some() {
            println!("Backup of \"{}\" is up to date. Skipping.\n", backup.volume);
            backup.end_snapshot = None;
        }
        else {
            let last_snapshot: &String = volume_snaps.last().unwrap();
            backup.end_snapshot = Some(last_snapshot[last_snapshot.find('@').unwrap() + 1 ..].to_string());
        }
    }

    backups.values()
}

fn interactive_backup(backups_dir: &str) {
    let mut volumes: Vec<Backup> = gather_volumes(backups_dir);

    let mut table = Table::new(&vec!("_", "volume", "incremental", "snapshot date"));
    for i in 0..volumes.len() {
        table.push(vec!(
                (i + 1).to_string(),
                volumes[i].volume.clone(),
                if volumes[i].start_snapshot.is_none() {
                    "full backup".to_string()
                }
                else {
                    volumes[i].start_snapshot.as_ref().unwrap().clone()
                },
                volumes[i].end_snapshot.as_ref().unwrap().clone()
            ));
    }

    //TODO
    println!("{}", table);
    println!("{:?}", volumes);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program_name = &args[0].rsplitn(2, '/').next().unwrap();

    let command = if args.len() < 2 {
        "help"
    }
    else {
        args[1].as_ref()
    };

    match command {
        "backup" => {
            if args.len() == 3 {
                interactive_backup(&args[2]);
            }
            else {
                println!("usage: {} backup <backups_location>", program_name);
                process::exit(-1);
            }
        },
        "automanage" => {
            println!("snapshot automanage is not yet implemented.");
            process::exit(-1);
        },
        _ => {
            if command != "help" {
                println!("unknown command \"{}\"", command);
            }
            println!("usage: {} <backup | automanage> [options]", program_name);
            process::exit(-1);
        }
    }
}
