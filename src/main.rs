// ZSnapMgr :: ZFS snapshot and backup manager
//
// Copyright (c) 2016 by William R. Fraser
//

#![allow(unknown_lints)] // for Clippy

use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::iter::Iterator;
use std::io;
use std::io::{Read, Write};
use std::ops::Deref;
use std::path::Path;
use std::process;

extern crate regex;
use regex::Regex;

extern crate termios;
use termios::*;

extern crate zsnapmgr;
use zsnapmgr::ZSnapMgr;

mod table;
use table::Table;

mod backups;
use backups::{Backup, Backups};

static USE_SUDO: bool = true;

// Print and flush.
macro_rules! printf {
    ( $fmt:expr ) => {
        print!($fmt);
        io::stdout().flush().unwrap();
    };
    ( $fmt:expr, $($arg:tt)* ) => {
        print!($fmt, $($arg)*);
        io::stdout().flush().unwrap();
    }
}

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

fn enumerate_files(path: &Path) -> Result<Box<dyn Iterator<Item = String>>, io::Error> {
    let readdir = fs::read_dir(path)?;
    let iter = readdir.filter_map(|entry_result| {
        match entry_result {
            Ok(entry) => {
                match entry.file_type() {
                    Ok(ft) => {
                        if ft.is_file() || ft.is_symlink() {
                            let path = entry.path();
                            let filename: &OsStr = path.file_name().unwrap();
                            match filename.to_str() {
                                Some(s) => Some(s.to_owned()),
                                None => {
                                    println!("error: filename {:?} is invalid UTF-8!", filename);
                                    None
                                }
                            }
                        } else {
                            None
                        }
                    },
                    Err(e) => {
                        println!("error getting file type of {:?}: {}",
                            entry.path(), e);
                        None
                    }
                }
            },
            Err(e) => {
                println!("error enumerating files: {}", e);
                None
            }
        }
    });
    Ok(Box::new(iter))
}

fn gather_volumes(path: &Path) -> Vec<Backup> {
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

    let file_iter = match enumerate_files(path) {
        Ok(x) => x,
        Err(e) => {
            println!("Error enumerating snapshot files: {}", e);
            return vec![];
        }
    };

    for file_path in file_iter {
        if let Some(zfs_pos) = file_path.find(".zfs") {
            if !(&file_path).ends_with("_partial") {
                let parts = file_path[0..zfs_pos].splitn(2, '@').collect::<Vec<&str>>();
                if parts.len() != 2 {
                    println!("ERROR: malformed ZFS filename: {:?}", file_path);
                    continue;
                }
                let volume_name = parts[0].replace("_", "/");
                let backup_snap = parts[1];

                if volumes.binary_search(&volume_name).is_ok() {
                    backups.insert(parts[0].to_string(),
                                   volume_name.to_string(),
                                   Some(backup_snap.to_string()));
                } else {
                    let volume_name_mod = "/".to_string() + &volume_name;
                    let matches: Vec<&str> = volumes.iter()
                                                    .filter(|vol| {
                                                        vol.ends_with(&volume_name_mod)
                                                    })
                                                    .map(Deref::deref)
                                                    .collect();

                    if matches.len() == 1 {
                        backups.insert(volume_name.to_string(),
                                       matches[0].to_string(),
                                       Some(backup_snap.to_string()));
                    } else {
                        print!("Backup filename \"{}\" ", file_path);
                        if matches.len() > 1 {
                            println!("matches more than one volume.\nIt could be any of: {:?}",
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

    let date_regex = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();

    // Now fill in the latest snapshot available for each volume in the proposed backups.
    for backup in backups.iter_mut() {
        let volume_at = backup.volume.clone() + "@";
        let mut volume_snaps: Vec<&str> = snapshots
                .iter()
                .filter(|snap| snap.starts_with(&volume_at)
                        && date_regex.is_match(snap.split_at(volume_at.len()).1))
                .map(Deref::deref)
                .collect();
        volume_snaps.sort();

        if backup.start_snapshot.is_some() {
            // Check that the start snapshot still exists.
            let start_snapshot = volume_at.clone() + backup.start_snapshot.as_ref().unwrap();

            if volume_snaps.binary_search(&start_snapshot.deref()).is_err() {
                println!("Snapshot \"{}\" doesn't exist any more; doing full backup instead.\n",
                         start_snapshot);
                backup.start_snapshot = None;
            }
        }

        let last_snapshot: &str = volume_snaps.last().unwrap().splitn(2, '@').last().unwrap();

        if backup.start_snapshot.as_deref() != Some(last_snapshot) {
            backup.end_snapshot = Some(last_snapshot.to_owned());
        } else if backup.start_snapshot.is_some() {
            println!("Backup of \"{}\" is up to date (@{}). Skipping.\n",
                     backup.volume,
                     backup.start_snapshot.as_ref().unwrap());
            backup.end_snapshot = None;
        }
    }

    backups.into_values()
}

fn getpass(prompt: &str) -> io::Result<String> {
    let mut termios = Termios::from_fd(0).expect("failed to get termios settings");

    let old = termios.c_lflag;
    termios.c_lflag &= !ECHO;   // disable echo
    termios.c_lflag &= !ICANON; // disable line-buffering
    tcsetattr(0, TCSAFLUSH, &termios).expect("failed to set termios settings");

    printf!("{}", prompt);

    let stdin = io::stdin();
    let mut bytes = stdin.lock().bytes();
    let mut line = String::new();
    let mut utf8 = Vec::<u8>::new();
    loop {
        match bytes.next().or_else(|| Some(Err(io::Error::new(io::ErrorKind::Other, "EOF in getpass!")))) {
            Some(Ok(byte)) => {
                // 0x4 is EOT; aka ctrl-D
                if byte == 0x4 && utf8.is_empty() {
                    return Err(io::Error::new(io::ErrorKind::Other, "EOF in getpass!"));
                }

                utf8.push(byte);

                let mut valid_utf8 = false;
                if let Ok(c) = std::str::from_utf8(&utf8) {
                    if c == "\n" {
                        println!();
                        break;
                    } else {
                        valid_utf8 = true;
                        line.push_str(c);
                        printf!("*");
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
    tcsetattr(0, TCSAFLUSH, &termios).expect("failed to reset termios settings");

    Ok(line)
}

fn do_backups(backups: &[Backup], path: &Path) {
    if backups.is_empty() {
        println!("Nothing to do.");
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

        println!("\nBacking up: {}", snapshot);

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

fn interactive_backup(backups_dir: &Path) {
    let z = ZSnapMgr::new(USE_SUDO);
    let mut backups: Vec<Backup> = gather_volumes(backups_dir);
    loop {
        let mut table = Table::new(&["_", "volume", "incremental", "snapshot date"]);
        for (i, backup) in backups.iter().enumerate() {
            let start = if backup.start_snapshot.is_none() {
                "full backup".to_string()
            } else {
                backup.start_snapshot.as_ref().unwrap().clone()
            };

            table.push(vec![(i + 1).to_string(),
                            backup.volume.clone(),
                            start,
                            backup.end_snapshot.as_ref().unwrap().clone()]);
        }

        println!("Volumes to backup:\n{}", table);

        printf!(concat!("Enter a number to make changes,\n",
                         "\t'+' to add a volume,\n",
                         "\t'-' to remove one,\n",
                         "\t'd' to change all dates,\n",
                         "\tor <return> to start backup: "));

        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    panic!("EOF");
                }
                input.pop();
            }
            Err(e) => panic!(e),
        }

        if input == "+" {
            printf!("Volume: ");

            let mut vol = String::new();
            io::stdin().read_line(&mut vol).unwrap();
            vol.pop();

            let latest_snap: String = match z.get_snapshots(Some(&vol))
                   .and_then(|ref mut snaps| {
                       Ok(snaps.pop()
                            .and_then(|full_name| {
                                full_name.rsplitn(2, '@')
                                    .next()
                                    .map(|s| s.to_owned())
                            }))
                   })
            {
                Ok(Some(date)) => date,
                Ok(None) => {
                    println!("No snapshots available for that volume.\n");
                    continue;
                }
                Err(e) => {
                    println!("Error listing snapshots: {}\n", e);
                    continue;
                }
            };

            backups.push(Backup {
                filename_base: vol.replace("/", "_").to_string(),
                volume: vol.clone(),
                start_snapshot: None,
                end_snapshot: Some(latest_snap),
            });

        } else if input.starts_with('-') {

            let index: usize;

            if input.len() > 1 {
                match input[1..].parse::<usize>() {
                    Ok(n) => {
                        index = n;
                    },
                    Err(e) => {
                        println!("Invalid number: {}\n", e);
                        continue;
                    }
                }
            } else {
                printf!("Remove which one?: ");

                input.clear();
                io::stdin().read_line(&mut input).unwrap();
                input.pop();

                match input.parse::<usize>() {
                    Ok(n) => {
                        index = n;
                    },
                    Err(e) => {
                        println!("Invalid number: {}\n", e);
                        continue;
                    }
                }
            }

            if backups.len() < index || index == 0 {
                println!("Number out of range.\n");
                continue;
            }

            backups.remove(index - 1);

        } else if input.starts_with('d') || input.starts_with('D') {

            printf!("Snapshot date (yyyy-MM-dd): ");

            let mut date = String::new();
            io::stdin().read_line(&mut date).unwrap();
            date.pop();

            for backup in &mut backups {
                backup.end_snapshot = Some(date.clone());
                //TODO: check that the snapshot exists for that volume
            }

        } else if input.is_empty() {
            println!("Starting backups.\n");
            do_backups(&backups, backups_dir);
            break;
        } else {
            let index: usize;

            match input.parse::<usize>() {
                Ok(n) => {
                    index = n;
                }
                Err(e) => {
                    println!("Invalid number: {}\n", e);
                    continue;
                }
            }

            if backups.len() < index || index == 0 {
                println!("Number out of range.\n");
                continue;
            }

            let vol = &mut backups[index - 1];

            printf!("Change (I)ncremental starting snapshot, (S)napshot date: ");

            input.clear();
            io::stdin().read_line(&mut input).unwrap();
            input.pop();

            let start: bool;
            if input == "I" || input == "i" {
                start = true;
            } else if input == "S" || input == "s" {
                start = false;
            } else {
                println!("Invalid selection.\n");
                continue;
            }

            printf!("Date (yyyy-MM-dd");
            if start {
                printf!(" or 'none' for full backup): ");
            } else {
                printf!("): ");
            }

            input.clear();
            io::stdin().read_line(&mut input).unwrap();
            input.pop();

            if start {
                vol.start_snapshot = if input == "none" {
                    None
                } else {
                    Some(input)
                }
            } else {
                vol.end_snapshot = Some(input);
            }
        }
    }
}

fn snapshot_automanage() {
    let z = ZSnapMgr::new(USE_SUDO);
    z.snapshot_automanage().unwrap();
}

fn main() {
    let args: Vec<OsString> = env::args_os().collect();
    let program_name = Path::new(&args[0]);

    let command = if args.len() < 2 {
        OsStr::new("help")
    } else {
        args[1].as_ref()
    };

    match command.to_str() {
        Some("backup") => {
            if args.len() == 3 {
                interactive_backup(Path::new(&args[2]));
            } else {
                println!("usage: {} backup <backups_location>", program_name.display());
                process::exit(-1);
            }
        }
        Some("automanage") => {
            snapshot_automanage();
        }
        _ => {
            if command != "help" {
                println!("unknown command \"{}\"", command.to_str().unwrap_or("[bad utf8]"));
            }
            println!("usage: {} <backup | automanage> [options]", program_name.display());
            process::exit(-1);
        }
    }
}
