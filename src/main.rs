#![cfg_attr(test, allow(dead_code))]

use std::collections::btree_map::{BTreeMap, Entry, IterMut};
use std::env;
use std::iter::Iterator;
use std::io;
use std::io::{Read, Write};
use std::ops::Deref;
use std::process;

extern crate termios;
use termios::*;

extern crate zsnapmgr;
use zsnapmgr::ZSnapMgr;

mod enumerate_files_no_error;
use enumerate_files_no_error::EnumerateFilesNoError;

mod table;
use table::Table;

static USE_SUDO: bool = true;

trait OptionDeref<T: Deref> {
    fn as_deref(&self) -> Option<&T::Target>;
}

impl<T: Deref> OptionDeref<T> for Option<T> {
    fn as_deref(&self) -> Option<&T::Target> {
        self.as_ref().map(Deref::deref)
    }
}

trait VecDeref<T: Deref> {
    fn as_deref(&self) -> Vec<&T::Target>;
}

impl<T: Deref> VecDeref<T> for Vec<T> {
    fn as_deref(&self) -> Vec<&T::Target> {
        self.iter().map(Deref::deref).collect()
    }
}

#[derive(Debug)]
struct Backup {
    filename_base: String,
    volume: String,
    start_snapshot: Option<String>,
    end_snapshot: Option<String>,
}

struct Backups {
    backups_by_volume: BTreeMap<String, Backup>,
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
        Backups { backups_by_volume: BTreeMap::new() }
    }

    pub fn insert(&mut self,
                  filename_base: String,
                  volume: String,
                  start_snapshot: Option<String>) {

        match self.backups_by_volume.entry(volume.clone()) {
            Entry::Occupied(ref mut entry) => {
                let backup = entry.get_mut();
                if start_snapshot.is_some() &&
                   (backup.start_snapshot.is_none() ||
                    start_snapshot.as_ref().unwrap() > backup.start_snapshot.as_ref().unwrap()) {
                    backup.start_snapshot = start_snapshot;
                    backup.filename_base = filename_base;
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(Backup {
                    filename_base: filename_base,
                    volume: volume,
                    start_snapshot: start_snapshot,
                    end_snapshot: None,
                });
            }
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
        BackupsIterMut { iter_mut: self.backups_by_volume.iter_mut() }
    }
}

fn gather_volumes(path: &str) -> Vec<Backup> {
    let z = ZSnapMgr::new(USE_SUDO);
    let snapshots: Vec<String> = match z.get_snapshots(None) {
        Ok(s) => s,
        Err(e) => {
            println!("Error getting snapshots from ZFS: {}", e);
            return vec![];
        }
    };

    let volumes: Vec<String> = match z.get_volumes() {
        Ok(v) => v,
        Err(e) => {
            println!("Error getting volumes from ZFS: {}", e);
            return vec![];
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
                            backups.insert(parts[0].to_string(),
                                           volume_name.to_string(),
                                           Some(backup_snap.to_string()));
                        } else {
                            let volume_name_mod = "/".to_string() + &volume_name;
                            let matches: Vec<&str> = volumes.iter()
                                                            .filter(|ref vol| {
                                                                vol.ends_with(&volume_name_mod)
                                                            })
                                                            .map(Deref::deref)
                                                            .collect();

                            if matches.len() == 1 {
                                backups.insert(matches[0].to_string(),
                                               volume_name.to_string(),
                                               Some(backup_snap.to_string()));
                            } else {
                                print!("Backup filename \"{}\" ", filename);
                                if matches.len() > 1 {
                                    println!("matches more than one volume.\nIt could be any of: \
                                              {:?}",
                                             matches);
                                } else {
                                    println!("doesn't match any volumes.");
                                }
                                println!("Skipping it.\n");
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            println!("Error listing directory \"{}\": {}", path, e);
            return vec![];
        }
    }

    // Now fill in the latest snapshot available for each volume in the proposed backups.
    for backup in backups.iter_mut() {
        let volume_at = backup.volume.clone() + "@";
        let volume_snaps: Vec<&str> = snapshots.iter()
                                               .filter(|snap| snap.starts_with(&volume_at))
                                               .map(Deref::deref)
                                               .collect();

        if backup.start_snapshot.is_some() {
            // Check that the start snapshot still exists.
            let start_snapshot = volume_at.clone() + backup.start_snapshot.as_ref().unwrap();

            if !volume_snaps.binary_search(&start_snapshot.deref()).is_ok() {
                println!("Snapshot \"{}\" doesn't exist any more; doing full backup instead.\n",
                         start_snapshot);
                backup.start_snapshot = None;
            }
        }

        let last_snapshot: &str = volume_snaps.last().unwrap().splitn(2, '@').last().unwrap();
        println!("last snapshot: {}", last_snapshot);

        if backup.start_snapshot
                 .as_ref()
                 .and_then(|start| Some(start != last_snapshot))
                 .unwrap_or(false) {
            backup.end_snapshot = Some(last_snapshot.to_string());
        } else {
            println!("Backup of \"{}\" is up to date (@{}). Skipping.\n",
                     backup.volume,
                     backup.start_snapshot.as_ref().unwrap());
            backup.end_snapshot = None;
        }
    }

    backups.values()
}

fn getpass(prompt: &str) -> io::Result<String> {
    let mut termios = Termios::from_fd(0).expect("failed to get termios settings");

    let old = termios.c_lflag;
    termios.c_lflag &= !ECHO;   // disable echo
    termios.c_lflag &= !ICANON; // disable line-buffering
    tcsetattr(0, TCSAFLUSH, &mut termios).expect("failed to set termios settings");

    let mut stdout = io::stdout();
    write!(stdout, "{}", prompt).unwrap();
    stdout.flush().unwrap();

    let stdin = io::stdin();
    let mut bytes = stdin.lock().bytes();
    let mut line = String::new();
    let mut utf8 = Vec::<u8>::new();
    loop {
        match bytes.next().or(Some(Err(io::Error::new(io::ErrorKind::Other, "EOF in getpass!")))) {
            Some(Ok(byte)) => {
                // 0x4 is EOT; aka ctrl-D
                if byte == 0x4 && utf8.is_empty() {
                    return Err(io::Error::new(io::ErrorKind::Other, "EOF in getpass!"));
                }

                utf8.push(byte);

                let mut valid_utf8 = false;
                if let Ok(c) = std::str::from_utf8(&utf8) {
                    if c == "\n" {
                        write!(stdout, "\n").unwrap();
                        stdout.flush().unwrap();
                        break;
                    } else {
                        valid_utf8 = true;
                        line.push_str(c);
                        write!(stdout, "*").unwrap();
                        stdout.flush().unwrap();
                    }
                }

                if valid_utf8 {
                    utf8.clear();
                }
            }
            Some(Err(e)) => {
                return Err(e);
            }
            _ => unreachable!(),
        }
    }

    termios.c_lflag = old;
    tcsetattr(0, TCSAFLUSH, &mut termios).expect("failed to reset termios settings");

    Ok(line)
}

fn do_backups(backups: &Vec<Backup>, path: &str) {
    if backups.is_empty() {
        return;
    }

    let passphrase: String;
    loop {
        let pass1 = getpass("GPG passphrase: ").unwrap();
        let pass2 = getpass("again: ").unwrap();
        if pass1 != pass2 {
            println!("Passphrases do not match.");
        } else {
            passphrase = pass1;
            break;
        }
    }

    for backup in backups {
        let z = ZSnapMgr::new(USE_SUDO);

        let snapshot = format!("{}@{}",
                               backup.volume,
                               backup.end_snapshot.as_deref().unwrap());
        z.backup(path,
                 &snapshot,
                 &passphrase,
                 backup.start_snapshot.as_deref())
         .err()
         .and_then(|e| {
             println!("failed backup of {}: {}", backup.volume, e);
             Some(())
         });
    }
}

fn interactive_backup(backups_dir: &str) {
    let mut volumes: Vec<Backup> = gather_volumes(backups_dir);

    loop {

        let mut table = Table::new(&vec!["_", "volume", "incremental", "snapshot date"]);
        for i in 0..volumes.len() {
            let start = if volumes[i].start_snapshot.is_none() {
                "full backup".to_string()
            } else {
                volumes[i].start_snapshot.as_ref().unwrap().clone()
            };

            table.push(vec![(i + 1).to_string(),
                            volumes[i].volume.clone(),
                            start,
                            volumes[i].end_snapshot.as_ref().unwrap().clone()]);
        }

        // TODO
        println!("{}", table);
        println!("{:?}", volumes);

        do_backups(&volumes, backups_dir);

        break;

    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program_name = &args[0].rsplitn(2, '/').next().unwrap();

    let command = if args.len() < 2 {
        "help"
    } else {
        args[1].as_ref()
    };

    match command {
        "backup" => {
            if args.len() == 3 {
                interactive_backup(&args[2]);
            } else {
                println!("usage: {} backup <backups_location>", program_name);
                process::exit(-1);
            }
        }
        "automanage" => {
            println!("snapshot automanage is not yet implemented.");
            process::exit(-1);
        }
        _ => {
            if command != "help" {
                println!("unknown command \"{}\"", command);
            }
            println!("usage: {} <backup | automanage> [options]", program_name);
            process::exit(-1);
        }
    }
}
