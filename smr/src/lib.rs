// Compilation slowed significantly
// by code and imports that appear dead
// because they are currently only used in tests
#![allow(dead_code)]
#![allow(unused_imports)]

extern crate hyper;
extern crate rustc_serialize;

pub mod runtime;
pub mod indexed_queue;
pub mod ds;
pub mod vm;
pub mod http_data;
pub mod http_server;
