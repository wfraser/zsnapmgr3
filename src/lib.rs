// ZSnapMgr :: ZFS snapshot and backup manager
//
// Copyright (c) 2016 by William R. Fraser
//

use std::collections::btree_map::*;
use std::ffi::OsString;
use std::io::Write;
use std::path::Path;

extern crate chrono;
extern crate libc;
extern crate libzfs;
extern crate ring;

use chrono::*;

#[macro_use] mod zfs_error; // include this first because its macros are used in other modules.
mod hash_stream;
mod inheritable_pipe;
mod zfs;

use inheritable_pipe::InheritablePipe;
use zfs::ZFS;
use zfs_error::ZfsError;

pub struct ZSnapMgr {
    zfs: ZFS,
}

fn date_from_snapshot(snap: &str) -> Option<Date<Local>> {
    let datepart = match snap.splitn(2, '@').last() {
        Some(s) => s,
        None => return None,
    };

    let dateparts: Vec<i32> = datepart.splitn(3, '-')
                                      .filter_map(|part| {
                                          part.parse::<i32>()
                                              .ok()
                                      })
                                      .collect();

    if dateparts.len() != 3 {
        return None;
    }

    Some(Local.ymd(dateparts[0], dateparts[1] as u32, dateparts[2] as u32))
}

trait Succ {
    fn succ(&self) -> Self;
}

trait WeekOfYear {
    fn week_of_year(&self) -> IsoWeek;
}

impl<Tz: TimeZone> Succ for Date<Tz> {
    fn succ(&self) -> Self {
        Date::<Tz>::succ(self)
    }
}

impl Succ for NaiveDate {
    fn succ(&self) -> Self {
        NaiveDate::succ(self)
    }
}

impl<T: Datelike + Succ> WeekOfYear for T {
    fn week_of_year(&self) -> IsoWeek {
        // The original C# version of this program used System.Globalization.Calendar.GetWeekOfYear
        // for this, using System.Globalization.DateTimeFormatInfo.InvariantInfo for the
        // parameters.
        //
        // "The first week of the year starts on the first day of the year and ends before the
        // following designated first day of the week."
        // The first day of the week is designated as Sunday.
        //
        // This is similar to the ISO week date, except that ISO week date has:
        //  - week starting on Monday
        //  - the first day of the year can be week 53 from the previous year (week 1 is defined as
        //    being the first week containing January 4 of that year)
        //
        // This isn't going to replicate the C# method exactly - the value can be +/- 1 depending
        // on which year it is for.
        // Figuring out whether to add 1 or not depending on the year is hard, and this method is
        // only used for finding the first snapshot in a week, so the difference isn't important,
        // as long as days in the same week get the same value.
        // In fact, the C# method wouldn't even uphold this property on the first week of the year
        // usually -- it would unconditionally change from 53 or 54 to 1 mid-week. This method does
        // not.

        if self.weekday() == Weekday::Sun {
            // We want weeks starting on Sunday, so if it's Sunday, use the ISO week number for
            // tomorrow.
            self.succ().iso_week()
        } else {
            self.iso_week()
        }
    }
}

impl ZSnapMgr {
    pub fn new(use_sudo: bool) -> Result<ZSnapMgr, ZfsError> {
        Ok(ZSnapMgr {
            zfs: ZFS::new(use_sudo)?,
        })
    }

    pub fn get_volumes(&self) -> Result<Vec<String>, ZfsError> {
        self.zfs.volumes(None)
    }

    pub fn get_snapshots(&self, dataset: Option<&str>) -> Result<Vec<String>, ZfsError> {
        self.zfs.snapshots(dataset)
    }

    pub fn backup(&self,
                  path: &Path,
                  snapshot: &str,
                  passphrase: &str,
                  incremental_start: Option<&str>)
                  -> Result<(), ZfsError> {
        let mut passphrase_pipe =
            zfstry!(InheritablePipe::new(), or "failed to create passphrase pipe");

        zfstry!(write!(passphrase_pipe, "{}\n", passphrase), or "failed to write passphrase to pipe");

        let destination_path = path.join(OsString::from(snapshot.replace("/", "_") + ".zfs.zst.gpg"));

        self.zfs.send(snapshot,
                      &destination_path,
                      incremental_start,
                      Some(&format!("zstd -T0 --size-hint=1000000000 | gpg --batch --symmetric --passphrase-fd {} \
                                     --output -",
                                    passphrase_pipe.child_fd())))
    }

    pub fn snapshot_automanage(&self) -> Result<(), ZfsError> {
        let today = Local::today();
        let today_str = format!("{:04}-{:02}-{:02}",
                                today.year(),
                                today.month(),
                                today.day());

        let mut all_snaps = self.get_snapshots(None)?;
        let mut snaps_map: BTreeMap<String, BTreeMap<Date<Local>, String>> = BTreeMap::new();
        for snap in all_snaps.drain(..) {
            let snap_date = match date_from_snapshot(&snap) {
                Some(date) => date,
                None => continue,
            };

            let volume = snap.splitn(2, '@').next().unwrap().to_string();
            let entry = snaps_map.entry(volume).or_insert_with(BTreeMap::new);
            entry.insert(snap_date, snap);
        }

        let mut to_delete = Vec::<String>::new();
        let mut to_create = Vec::<String>::new();

        for (volume, snaps) in snaps_map {
            let mut count = 0;

            for (snap_date, snap) in snaps.iter().rev() {
                count += 1;

                let days_old = (today.signed_duration_since(*snap_date)).num_days();

                if (count == 1) && (days_old != 0) {
                    println!("{}\t{}\t0 days old\t#1\t[NEW]", volume, today_str);
                    to_create.push(format!("{}@{}", volume, today_str));
                    count += 1;
                }

                print!("{}\t{}\t{} days old\t#{}",
                       volume,
                       snap.splitn(2, '@').last().unwrap(),
                       days_old,
                       count);

                // Give the tuple elements names.
                struct Pair<'a> {
                    date: &'a Date<Local>,
                    snap: &'a str,
                };

                const ISO8601_DATE_FMT: &str = "%Y-%m-%d";

                let mut delete = None::<String>; // set to Some(reason) if deletion should happen

                let first_of_month = snaps.iter()
                                          .map(|(date, snap)| Pair { date, snap })
                                          .find(|pair| {
                                              pair.date.year() == snap_date.year() &&
                                              pair.date.month() == snap_date.month()
                                          })
                                          .unwrap();

                if count > 60 {
                    // Keep only the first snapshot of the month.
                    if first_of_month.snap != snap {
                        delete = Some(format!("not first of month ({})",
                            first_of_month.date.format(ISO8601_DATE_FMT)));
                    }
                } else if count > 30 {
                    // Keep only the first snapshot of the week or month.
                    let first_of_week = snaps.iter()
                                             .map(|(date, snap)| Pair { date, snap })
                                             .find(|pair| {
                                                 pair.date.week_of_year() == snap_date.week_of_year()
                                             })
                                             .unwrap();

                    if first_of_week.snap != snap &&
                       first_of_month.snap != snap {
                        delete = Some(format!("not first of month ({}) or first of week ({})",
                            first_of_month.date.format(ISO8601_DATE_FMT),
                            first_of_week.date.format(ISO8601_DATE_FMT)));
                    }
                }

                if let Some(why) = delete {
                    print!("\t[DELETE] {}", why);
                    to_delete.push(snap.to_string());
                }

                println!();
            }
        }

        for snap in to_delete {
            // TODO
            println!("ZFS DELETE {}", snap);
        }

        for snap in to_create {
            // TODO
            println!("ZFS SNAPSHOT {}", snap);
        }

        Err(ZfsError::from("snapshot automanage is not yet implemented."))
    }
}
