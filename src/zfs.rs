// ZFS :: Interface to the `zfs` client library.
//
// Copyright (c) 2016-2021 by William R. Fraser
//

use std::cmp;
use std::fs;
use std::process::{Child, Command, Stdio};
use std::io::{stdout, Error, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use crate::hash_stream;
use crate::zfs_error::ZfsError;

use chrono::prelude::*;
use ring::digest::SHA256;

use libzfs::{DatasetType, DatasetTypeMask, LibZfs};

pub struct Zfs {
    client: LibZfs,
    pub use_sudo: bool,
}

fn read_line<R: Read>(r: &mut R) -> Result<Option<String>, Error> {
    let mut line = String::new();
    loop {
        let mut buf = [0u8; 1];
        match r.read(&mut buf) {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    if line.is_empty() {
                        return Ok(None);
                    } else {
                        return Ok(Some(line));
                    }
                } else if buf[0] == b'\n' {
                    return Ok(Some(line));
                } else {
                    line.push(buf[0] as char);   // we're assuming pure ASCII here.
                }
            }
            Err(e) => return Err(e),
        }
    }
}

fn human_number(n: u64, decimals: usize) -> String {
    if n == 0 {
        return "0".to_string();
    }

    let magnitude = (n as f64).log(1000_f64).floor() as i32;
    if magnitude == 0 {
        return n.to_string();
    }

    let suffixes = ['k', 'M', 'G', 'T', 'P', 'E'];

    let h = (n as f64) / 1000_f64.powi(magnitude);
    if magnitude > 0 {
        format!("{:.*} {}", decimals, h, suffixes[magnitude as usize - 1])
    } else {
        h.to_string()
    }
}

#[test]
#[rustfmt::skip]
fn test_human_number() {
    assert_eq!(human_number(            1, 1), "1");
    assert_eq!(human_number(          999, 1), "999");
    assert_eq!(human_number(         1000, 1), "1.0 k");
    assert_eq!(human_number(         1500, 1), "1.5 k");
    assert_eq!(human_number(       999900, 1), "999.9 k");
    assert_eq!(human_number(      1000000, 1), "1.0 M");
    assert_eq!(human_number(   1000000000, 1), "1.0 G");
    assert_eq!(human_number(1000000000000, 1), "1.0 T");
}

fn exclude_dataset(_ds: &libzfs::Dataset) -> bool {
    // TODO: exclude ones with the 'zsnapmgr:noautosnap' property
    false
}

impl Zfs {
    pub fn new(use_sudo: bool) -> Result<Self, ZfsError> {
        let client = libzfs::LibZfs::new()?;
        Ok(Self {
            client,
            use_sudo,
        })
    }

    pub fn volumes(&self, pool: Option<&str>) -> Result<Vec<String>, ZfsError> {
        // for purposes of this program, "volumes" is defined as filesystems + zvols
        let mut volumes = vec![];
        let pool_names = if let Some(name) = pool {
            vec![name.into()]
        } else {
            self.client.get_zpools()?
                .into_iter()
                .map(|pool| pool.get_name()).collect()
        };
        for pool_name in pool_names {
            let pool_dataset = self.client.dataset_by_name(&pool_name, DatasetTypeMask::all())?;
            for dataset in pool_dataset.get_all_dependents()? {
                if exclude_dataset(&dataset) {
                    continue;
                }
                match dataset.get_type() {
                    DatasetType::Filesystem | DatasetType::Volume => {
                        volumes.push(dataset.get_name().to_string());
                    },
                    _ => (),
                }
            }
        }
        Ok(volumes)
    }

    pub fn snapshots(&self, dataset: Option<&str>) -> Result<Vec<String>, ZfsError> {
        match dataset {
            Some(name) => {
                let ds = self.client.dataset_by_name(&name.into(), DatasetTypeMask::all())?;
                Ok(ds.get_snapshots()?
                    .into_iter()
                    .filter_map(|ds| {
                        if exclude_dataset(&ds) {
                            None
                        } else {
                            Some(ds.get_name().to_string())
                        }
                    })
                    .collect())
            }
            None => {
                let mut snapshots = vec![];
                for pool in self.client.get_zpools()? {
                    let pool_ds = self.client.dataset_by_name(&pool.get_name(), DatasetTypeMask::all())?;
                    snapshots.extend(
                        pool_ds.get_all_dependents()?
                            .into_iter()
                            .filter_map(|ds| {
                                // TODO: exclude ones with the 'zsnapmgr:noautosnap=yes' property
                                if exclude_dataset(&ds) {
                                    None
                                } else if ds.get_type() == DatasetType::Snapshot {
                                    Some(ds.get_name().to_string())
                                } else {
                                    None
                                }
                            })
                        );
                }
                Ok(snapshots)
            }
        }
    }

    pub fn create_snapshots<I: Iterator<Item=T>, T: AsRef<str>>(&self, names: I) -> Result<(), ZfsError> {
        self.client.create_snapshots(names)?;
        Ok(())
    }

    pub fn destroy_snapshots<I, T>(&self, names: I) -> Result<(), ZfsError>
        where I: Iterator<Item=T>,
              T: AsRef<str>,
    {
        self.client.destroy_snapshots(names)?;
        Ok(())
    }

    pub fn send(&self,
                snapshot: &str,
                destination_path: &Path,
                incremental: Option<&str>,
                filter_program: Option<&str>)
                -> Result<(), ZfsError> {

        // This uses 'sh -c' to run the pipeline because it's less work for us.
        // The "$0" and "$1" are replaced by the additional arguments passed to sh.
        // This is nice because it means they can contain any characters and require no escaping.

        let cmdline = format!("{} send --parsable --verbose {} $1 {}{}",
            if self.use_sudo { "sudo zfs" } else { "zfs" },
            if incremental.is_some() { "-i @$0" } else { "" },
            if filter_program.is_some() { " | " } else { "" },
            filter_program.unwrap_or("")
        );

        let mut partial_filename = destination_path.file_name().unwrap().to_os_string();
        partial_filename.push("_partial");
        let partial_path = destination_path.with_file_name(&partial_filename);

        println!("running: {}",
            cmdline
                .replace("$0", incremental.as_deref().unwrap_or(""))
                .replace("$1", snapshot)
        );
        let mut child: Child = zfstry!(Command::new("sh")
            .arg("-c")
            .arg(&cmdline)
            .arg(incremental.or(Some("")).unwrap())
            .arg(snapshot)
            .stdin(Stdio::inherit())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn(), or "failed to run 'zfs send'");

        let mut backup_out = child.stdout.take();
        let partial_path2 = partial_path.clone();

        let mut sidecar_filename = partial_filename;
        sidecar_filename.push(".sha256sum");
        let partial_sidecar_path = partial_path.with_file_name(sidecar_filename);
        let partial_sidecar_path2 = partial_sidecar_path.clone();

        let mut destination_sidecar_filename = destination_path.file_name().unwrap().to_os_string();
        destination_sidecar_filename.push(".sha256sum");
        let destination_sidecar_path = destination_path.with_file_name(destination_sidecar_filename);

        let output_progress = Arc::new(hash_stream::AtomicU64::new(0));
        let output_progress_hashthread = Arc::clone(&output_progress);
        let read_thread = thread::spawn(move || {
            if let Err(e) = hash_stream::write_file_and_sidecar(
                backup_out.as_mut().unwrap(),
                &partial_path2,
                &partial_sidecar_path2,
                &SHA256,
                &output_progress_hashthread)
            {
                let msg = format!("Error reading/writing 'zfs send' pipeline: {}", e);
                println!("{}", msg);
                panic!("{}", msg);
            }
        });

        let mut size: u64 = 0;
        let mut last_line_length: isize = 0;
        let start_time = Local::now();
        loop {
            match read_line(child.stderr.as_mut().unwrap()) {
                Ok(Some(line)) => {
                    if (&line).starts_with("incremental\t") || (&line).starts_with("full\t") {
                        continue;
                    }
                    if (&line).starts_with("size\t") {
                        size = (&line).split_at(5).1.parse::<u64>().unwrap();
                        println!("Full size: {}B", human_number(size, 1));
                        if size == 0 {
                            println!("Empty snapshot; skipping.");
                            break;
                        }
                    } else {
                        let parts: Vec<&str> = line.split('\t').collect();
                        if parts.len() != 3 {
                            let msg = format!("Unrecognized output: {}", line);
                            return Err(ZfsError::from(msg));
                        }

                        let time_parts: Vec<u32> = parts[0].split(':')
                                                           .filter_map(|x| x.parse::<u32>().ok())
                                                           .collect();
                        if time_parts.len() != 3 {
                            let msg = format!("Unrecognized output: {}", line);
                            return Err(ZfsError::from(msg));
                        }
                        let time = Local::today().and_hms(time_parts[0],
                                                          time_parts[1],
                                                          time_parts[2]);
                        let elapsed = time.signed_duration_since(start_time);

                        let partial_size: u64;
                        if let Ok(n) = parts[1].parse::<u64>() {
                            partial_size = n;
                        } else {
                            let msg = format!("Unrecognized output: {}", line);
                            return Err(ZfsError::from(msg));
                        }

                        let output_size = output_progress.load(::std::sync::atomic::Ordering::Relaxed);
                        let compratio: f64 = 100. - (output_size as f64) / (partial_size as f64) * 100.;

                        let percent: f64 = (partial_size as f64) / (size as f64) * 100.;
                        let outline = format!("{:02}:{:02}:{:02} {:.1}% {}B in {}B out ({:.1}% compressed)",
                                              elapsed.num_hours(),
                                              elapsed.num_minutes() % 60,
                                              elapsed.num_seconds() % 60,
                                              percent,
                                              human_number(partial_size, 1),
                                              human_number(output_size, 1),
                                              compratio);
                        let spacing =
                            cmp::max(0, last_line_length - outline.len() as isize) as usize;
                        print!("\r{}{}", outline, " ".repeat(spacing));
                        zfstry!(stdout().flush(), or "failed to flush stdout?!");
                        last_line_length = outline.len() as isize;
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(ZfsError::from(("error reading from 'zfs send' pipeline", e))),
            }
        }
        println!();

        if let Err(e) = read_thread.join() {
            println!("read thread died");
            let msg: &str = e.downcast_ref::<String>().unwrap().as_str();
            return Err(ZfsError::from(msg));
        }

        let exit_status = (&mut child).wait().unwrap();
        if !exit_status.success() {
            let code = exit_status.code().or(Some(0)).unwrap();
            return Err(ZfsError::from(format!("'zfs send' returned nonzero exit code: {}", code)));
        }

        if size == 0 {
            zfstry!(fs::remove_file(&partial_path), or "failed to remove empty partial file");
        } else {
            zfstry!(fs::rename(&partial_path, &destination_path),
                or "failed to move partial file to destination");
            zfstry!(fs::rename(&partial_sidecar_path, &destination_sidecar_path),
                or "failed to move partial file sidecar to destination");
            let mut sidecar = zfstry!(
                fs::OpenOptions::new().append(true).open(&destination_sidecar_path),
                    or "failed to update hash sidecar (1)");

            let mut bytes = b" *".to_vec();
            bytes.extend_from_slice(destination_path.file_name().unwrap().as_bytes());
            bytes.extend_from_slice(b"\n");
            zfstry!(sidecar.write_all(&bytes), or "failed to update hash sidecar (2)");
        }

        Ok(())
    }
}
