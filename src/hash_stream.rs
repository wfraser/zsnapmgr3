use ring::digest::*;

use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(target_pointer_width = "64")]
//pub type AtomicU64 = AtomicUsize;
pub struct AtomicU64 {
    inner: AtomicUsize,
}

#[cfg(target_pointer_width = "64")]
impl AtomicU64 {
    pub fn new(initial: u64) -> Self {
        Self {
            inner: AtomicUsize::new(initial as usize),
        }
    }

    pub fn load(&self, order: Ordering) -> u64 {
        self.inner.load(order) as u64
    }

    pub fn fetch_add(&self, val: u64, order: Ordering) -> u64 {
        self.inner.fetch_add(val as usize, order) as u64
    }
}


#[cfg(not(target_pointer_width = "64"))]
pub type AtomicU64 = !; // until the proper AtomicU64 type is stable, this needs to be compiled 64-bit.

pub struct HashingWrite<T: Write> {
    ctx: Context,
    inner: T,
}

impl<T: Write> HashingWrite<T> {
    pub fn new(inner: T, algo: &'static Algorithm) -> HashingWrite<T> {
        HashingWrite {
            ctx: Context::new(algo),
            inner,
        }
    }

    pub fn finish(self) -> Vec<u8> {
        self.ctx.finish().as_ref().to_vec()
    }
}

pub fn write_file_and_sidecar<R: Read>(
    input: &mut R,
    path: &Path,
    sidecar_path: &Path,
    algo: &'static Algorithm,
    progress: &AtomicU64,
    ) -> Result<(), String>
{
    /*
    let mut hash_filename = path.file_name().unwrap().to_os_string();
    hash_filename.push(suffix);
    let hash_file_path = path.with_file_name(hash_filename);
    */

    let out = File::create(path).map_err(|e| format!("failed to create {:?}: {}", path, e))?;
    let mut hash_out = HashingWrite::new(out, algo);

    let mut buf = [0u8; 8192];
    loop {
        match input.read(&mut buf) {
            Ok(0) => break,
            Ok(nread) => {
                progress.fetch_add(nread as u64, Ordering::Relaxed);
                match hash_out.write(&buf[0..nread]) {
                    Ok(nwritten) => {
                        if nwritten != nread {
                            let msg = format!("nwritten({}) != nread({})", nwritten, nread);
                            return Err(msg);
                        }
                    },
                    Err(e) => {
                        return Err(format!("write error: {}", e));
                    }
                }
            },
            Err(e) => {
                return Err(format!("read error: {}", e));
            }
        }
    }

    let hash: String = hash_out.finish().iter()
        .fold(String::new(), |s, byte| s + &format!("{:02x}", byte));

    let mut sidecar_file = match File::create(sidecar_path) {
        Ok(f) => f,
        Err(e) => {
            let msg = format!("failed to create sidecar {:?}: {}", sidecar_path, e);
            return Err(msg);
        }
    };

    if let Err(e) = sidecar_file.write_all(hash.as_bytes()) {
        return Err(format!("failed to write hash sidecar {:?}: {}", sidecar_path, e));
    }

    Ok(())
}

impl<T: Write> Write for HashingWrite<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.ctx.update(buf);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
