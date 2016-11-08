// ZFS :: Interface to the `zfs` command line program.
//
// Copyright (c) 2016 by William R. Fraser
//

use std::cmp;
use std::fs;
use std::process::{Child, Command, Stdio};
use std::io::{stdout, Error, Read, Write};
use std::iter::repeat;
use std::path::Path;

use zfs_error::ZfsError;

extern crate chrono;
use chrono::*;

pub struct ZFS {
    pub use_sudo: bool,
}

fn get_first_column(bytes: &[u8]) -> Vec<String> {
    let mut results: Vec<String> = Vec::new();

    for line in String::from_utf8_lossy(bytes).lines() {
        if !line.trim().is_empty() {
            let mut split = line.splitn(2, '\t');
            if let (Some(field), Some(noautosnap)) = (split.next(), split.next()) {
                if !noautosnap.starts_with("yes") {
                    results.push(String::from(field));
                }
            }
        }
    }

    results
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
#[cfg_attr(rustfmt, rustfmt_skip)]
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

impl ZFS {
    fn zfs_command(&self) -> Command {
        if self.use_sudo {
            let mut cmd = Command::new("sudo");
            cmd.arg("zfs");
            cmd
        } else {
            Command::new("zfs")
        }
    }

    fn zfs_list(&self, result_type: &str, volume: Option<&str>) -> Result<Vec<String>, ZfsError> {
        let mut cmd = self.zfs_command();
        cmd.arg("list")
           .arg("-H")
           .arg("-t")
           .arg(result_type)
           .arg("-o")
           .arg("name,zsnapmgr:noautosnap");
        if volume.is_some() {
            cmd.arg("-r")
               .arg("-d")
               .arg("1")
               .arg(volume.unwrap());
        }

        match cmd.output() {
            Err(e) => Err(ZfsError::from(("failed to run 'zfs list'", e))),
            Ok(result) => {
                if !result.status.success() {
                    return Err(ZfsError::from(("failed to run 'zfs list'", &result.stderr)));
                }

                Ok(get_first_column(&result.stdout))
            }
        }
    }

    pub fn volumes(&self, pool: Option<&str>) -> Result<Vec<String>, ZfsError> {
        self.zfs_list("filesystem", pool)
    }

    pub fn snapshots(&self, dataset: Option<&str>) -> Result<Vec<String>, ZfsError> {
        self.zfs_list("snapshot", dataset)
    }

    pub fn send(&self,
                snapshot: &str,
                destination_path: &Path,
                incremental: Option<&str>,
                filter_program: Option<&str>)
                -> Result<(), ZfsError> {

        // This uses 'sh -c' to run the pipeline because it's less work for us.
        // The "$0", "$1", "$2" are replaced by the additional arguments passed to sh.
        // This is nice because it means they can contain any characters and require no escaping.

        let cmdline = format!("{} send -P -v {} $1 {}{} > $2",
            if self.use_sudo { "sudo zfs" } else { "zfs" },
            if incremental.is_some() { "-i $0" } else { "" },
            if filter_program.is_some() { " | " } else { "" },
            filter_program.unwrap_or("")
        );

        let mut partial_filename = destination_path.file_name().unwrap().to_os_string();
        partial_filename.push("_partial");
        let partial_path = destination_path.with_file_name(partial_filename);

        let mut child: Child = zfstry!(Command::new("sh")
            .arg("-c")
            .arg(&cmdline)
            .arg(incremental.or(Some("")).unwrap())
            .arg(snapshot)
            .arg(&partial_path)
            .stdin(Stdio::inherit())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn(), or "failed to run 'zfs send'");

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
                        let elapsed = time - start_time;

                        let partial_size: u64;
                        if let Ok(n) = parts[1].parse::<u64>() {
                            partial_size = n;
                        } else {
                            let msg = format!("Unrecognized output: {}", line);
                            return Err(ZfsError::from(msg));
                        }

                        let percent: f64 = (partial_size as f64) / (size as f64) * 100.;
                        let outline = format!("{:02}:{:02}:{:02} {:.1}% {}B",
                                              elapsed.num_hours(),
                                              elapsed.num_minutes() % 60,
                                              elapsed.num_seconds() % 60,
                                              percent,
                                              human_number(partial_size, 1));
                        let spacing =
                            cmp::max(0, last_line_length - outline.len() as isize) as usize;
                        print!("\r{}{}",
                               outline,
                               repeat(' ').take(spacing).collect::<String>());
                        zfstry!(stdout().flush(), or "failed to flush stdout?!");
                        last_line_length = outline.len() as isize;
                    }
                }
                Ok(None) => break,
                Err(e) => return Err(ZfsError::from(("error reading from 'zfs send' pipeline", e))),
            }
        }

        let exit_status = (&mut child).wait().unwrap();
        if !exit_status.success() {
            let code = exit_status.code().or(Some(0)).unwrap();
            return Err(ZfsError::from(format!("'zfs send' returned nonzero exit code: {}", code)));
        }

        if size == 0 {
            zfstry!(fs::remove_file(&partial_path), or "failed to remove empty partial file");
        } else {
            zfstry!(fs::rename(&partial_path, destination_path), or "failed to move partial file to destination");
        }

        Ok(())
    }
}
