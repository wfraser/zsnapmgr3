// ZfsError :: Custom error messages for ZSnapMgr
//
// Copyright (c) 2016 by William R. Fraser
//

use std::error;
use std::fmt;
use std::io::{Error, ErrorKind};

macro_rules! zfstry {
    ( $e:expr, or $msg:expr ) => {
        match $e {
            Ok(result) => result,
            Err(err) => return Err(ZfsError::from(($msg, err))),
        }
    }
}

pub struct ZfsError {
    pub descr: String,
    pub io_error: Option<Error>,
}

impl error::Error for ZfsError {
    fn description(&self) -> &str {
        &self.descr
    }
    fn cause(&self) -> Option<&dyn error::Error> {
        match self.io_error {
            Some(ref e) => Some(e),
            None => None,
        }
    }
}

impl fmt::Debug for ZfsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if self.io_error.is_some() {
            write!(f,
                   "[ZfsError] {}: {}",
                   self.descr,
                   self.io_error.as_ref().unwrap())
        } else {
            write!(f, "[ZfsError] {}", self.descr)
        }
    }
}

impl fmt::Display for ZfsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if self.io_error.is_some() {
            write!(f, "{}: {}", self.descr, self.io_error.as_ref().unwrap())
        } else {
            write!(f, "{}", self.descr)
        }
    }
}

impl From<(String, Error)> for ZfsError {
    fn from(args: (String, Error)) -> ZfsError {
        ZfsError {
            descr: args.0,
            io_error: Some(args.1),
        }
    }
}

impl<'a> From<(&'a str, Error)> for ZfsError {
    fn from(args: (&'a str, Error)) -> ZfsError {
        ZfsError {
            descr: String::from(args.0),
            io_error: Some(args.1),
        }
    }
}

impl<'a, 'b> From<(&'a str, &'b Vec<u8>)> for ZfsError {
    fn from(args: (&'a str, &'b Vec<u8>)) -> ZfsError {
        ZfsError {
            descr: String::from(args.0),
            io_error: Some(Error::new(ErrorKind::Other,
                                      (*String::from_utf8_lossy(args.1)).to_owned())),
        }
    }
}

impl<'a> From<&'a str> for ZfsError {
    fn from(descr: &'a str) -> ZfsError {
        ZfsError {
            descr: String::from(descr),
            io_error: None,
        }
    }
}

impl From<String> for ZfsError {
    fn from(descr: String) -> ZfsError {
        ZfsError {
            descr: descr,
            io_error: None,
        }
    }
}
