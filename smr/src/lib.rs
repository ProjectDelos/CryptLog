#![allow(dead_code)]
#![feature(custom_derive, plugin)]
#![plugin(serde_macros)]

extern crate hyper;
extern crate serde;
extern crate serde_json;
extern crate rustc_serialize;

pub mod runtime;
pub mod indexed_queue;
pub mod ds;
pub mod vm;
pub mod http_data;
pub mod http_server;
