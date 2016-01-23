use std::fs;
use std::io;
use std::path;

pub struct EnumerateFilesNoError {
    read_dir: fs::ReadDir,
}

impl EnumerateFilesNoError {
    pub fn new(path: &str) -> io::Result<EnumerateFilesNoError> {
        let read_dir = fs::read_dir(path::Path::new(path));
        if read_dir.is_err() {
            return Err(read_dir.err().unwrap());
        }
        else {
            Ok(EnumerateFilesNoError {
                read_dir: read_dir.unwrap()
            })
        }
    }
}

impl Iterator for EnumerateFilesNoError {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        loop {
            match self.read_dir.next() {
                Some(Ok(entry)) => {
                    if let Ok(metadata) = fs::metadata(entry.path()) {
                        if metadata.is_file() {
                            return Some(entry.file_name().to_string_lossy().into_owned())
                        }
                    }
                },
                Some(Err(_)) => (), // ignore the error; loop again.
                None => return None,
            }
        }
    }
}
