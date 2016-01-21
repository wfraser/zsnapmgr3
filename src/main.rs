#![cfg_attr(test, allow(dead_code))]

use std::collections::BTreeMap;
use std::env;
use std::iter::Iterator;
use std::process;

extern crate zsnapmgr;
use zsnapmgr::ZSnapMgr;

mod enumerate_files_no_error;
use enumerate_files_no_error::EnumerateFilesNoError;

static USE_SUDO: bool = true;

#[derive(Debug)]
struct Backup {
    filename_base: String,
    volume: String,
    latest_snapshot: String,
}

struct Backups {
    backups_by_volume: BTreeMap<String, Backup>
}

impl Backups {
    pub fn new() -> Backups {
        Backups {
            backups_by_volume: BTreeMap::new()
        }
    }

    pub fn insert(&mut self, filename: &str, snapshot: &str) {
        let filename_parts: Vec<&str> = filename.splitn(2, '@').collect();
        let volume: &str = snapshot.splitn(2, '@').next().unwrap();

        let mut found = false;
        if let Some(ref mut backup) = self.backups_by_volume.get_mut(volume) {
            found = true;
            if filename_parts[1] > &backup.latest_snapshot {
                backup.latest_snapshot = filename_parts[1].to_string();
                backup.filename_base   = filename_parts[0].to_string();
            }
        }
        if !found {
            self.backups_by_volume.insert(volume.to_string(), Backup {
                filename_base: filename_parts[0].to_string(),
                volume: volume.to_string(),
                latest_snapshot: filename_parts[1].to_string(),
            });
        }
    }

    pub fn values(self) -> Vec<Backup> {
        let mut vec: Vec<Backup> = Vec::new();
        for (_k, v) in self.backups_by_volume.into_iter() {
            vec.push(v);
        }
        vec
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

    let mut backups = Backups::new();

    match EnumerateFilesNoError::new(path) {
        Ok(enumerator) => {
            for filename in enumerator {
                if let Some(pos) = filename.find(".zfs") {
                    if !(&filename).ends_with("_partial") {
                        let snap_name = filename[0..pos].replace("_", "/");
                        if snapshots.binary_search(&snap_name).is_ok() {
                            // exact match of a snapshot name
                            backups.insert(&filename[0..pos], &snap_name);
                        }
                        else {
                            // see if the filename matches the end of a snapshot
                            let snap_name_mod = "/".to_string() + &snap_name;

                            let matches = snapshots.iter().filter(|item| {
                                item.ends_with(&snap_name_mod)
                            }).collect::<Vec<&String>>();

                            if matches.len() == 1 {
                                backups.insert(&filename[0..pos], matches[0]);
                            }
                            else {
                                print!("Backup filename \"{}\" doesn't match a snapshot.", filename);
                                if matches.len() > 1 {
                                    print!(" It could be any of: {:?}.", matches);
                                }
                                println!(" Skipping it.");
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

    backups.values()
}

fn interactive_backup(backups_dir: &str) {
    //TODO
    println!("{:?}", gather_volumes(backups_dir));
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
