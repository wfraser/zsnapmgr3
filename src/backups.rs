// Backup configuration structs
//
// Copyright (c) 2016 by William R. Fraser
//

use std::collections::btree_map::{BTreeMap, Entry, IterMut};

#[derive(Debug)]
pub struct Backup {
    pub filename_base: String,
    pub volume: String,
    pub start_snapshot: Option<String>,
    pub end_snapshot: Option<String>,
}

pub struct Backups {
    backups_by_volume: BTreeMap<String, Backup>,
}

pub struct BackupsIterMut<'a> {
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

    #[allow(for_kv_map)]
    pub fn into_values(self) -> Vec<Backup> {
        let mut vec: Vec<Backup> = Vec::new();
        for (_k, v) in self.backups_by_volume {
            if v.end_snapshot.is_some() {
                vec.push(v);
            }
        }
        vec
    }

    pub fn iter_mut(&mut self) -> BackupsIterMut {
        BackupsIterMut { iter_mut: self.backups_by_volume.iter_mut() }
    }
}
