// InheritablePipe :: A pipe that can be used to communicate with a child process.
//
// Copyright (c) 2016 by William R. Fraser
//

use std::fs::File;
use std::io::{Error, Read, Result, Write};
use std::os::unix::io::FromRawFd;

use libc::{self, c_int};

pub struct InheritablePipe {
    child_fd: c_int,
    our_file: File,
}

// this is a macro just so it can handle any integer type easily.
macro_rules! check_err {
    ( $e:expr ) => {
        match $e {
            -1 => Err(Error::last_os_error()),
            other => Ok(other)
        }
    }
}

impl InheritablePipe {
    pub fn new() -> Result<InheritablePipe> {
        let mut fds = [-1, -1];
        unsafe {
            check_err!(libc::pipe(&mut fds[0] as *mut c_int))?;

            // Set the FD_CLOEXEC flag on our end of the pipe, but not the child end.
            let flags = check_err!(libc::fcntl(fds[0], libc::F_GETFD))?;
            check_err!(libc::fcntl(fds[1], libc::F_SETFD, flags | libc::FD_CLOEXEC))?;

            Ok(InheritablePipe {
                child_fd: fds[0],
                our_file: File::from_raw_fd(fds[1]),
            })
        }
    }

    pub fn child_fd(&self) -> c_int {
        self.child_fd
    }

    pub fn close_child_fd(&mut self) -> Result<()> {
        if self.child_fd != -1 {
            unsafe {
                check_err!(libc::close(self.child_fd))?;
            }
            self.child_fd = -1;
        }
        Ok(())
    }
}

impl Write for InheritablePipe {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.our_file.write(buf)
    }
    fn flush(&mut self) -> Result<()> {
        self.our_file.flush()
    }
}

impl Read for InheritablePipe {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.our_file.read(buf)
    }
}

impl Drop for InheritablePipe {
    fn drop(&mut self) {
        self.close_child_fd().expect("failed to close child fd");
    }
}
