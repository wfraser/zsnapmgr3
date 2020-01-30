zsnapmgr3
=========

# This program is a work-in-progress.

This is my ZFS automatic management program. It has two main functions:
1. Automatically do daily snapshots of filesystems and thin out old snapshots
   as they age and become less needed, intended to be run as a cron job.
2. An interactive incremental backup mode, which compresses, encrypts, and
   checksums the resulting files.

This is the 3rd iteration of this program.

Version 1 was written in C#: https://github.com/wfraser/zsnapmgr

Version 2 was rewritten in Rust. https://github.com/wfraser/zsnapmgr_rust

These two versions both use the ZFS command-line program to do their work.

This version is an attempt to use the libzfs library directly, so an important
part of it is the `zfs-rs` submodule, which is in its own git repository:
https://github.com/wfraser/libzfs-rs
