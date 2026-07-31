#![forbid(unsafe_code)]

pub mod application;
pub mod cli;
pub mod config;
pub mod constants;
pub mod contract;
pub mod core;
mod digest;
pub mod domain;
pub mod error;
pub mod ipc;
pub mod lifecycle;
pub mod mihomo;
pub mod persistence;
pub mod profile;
pub mod profile_source;
pub mod rule;
pub mod runtime_bundle;
pub mod scheduler;
pub mod service;
pub mod state;
pub mod telemetry;
pub mod transaction;
pub mod tui;
pub mod validator;
