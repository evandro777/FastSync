//! UI-independent logic: the sync engine, its DB, the rclone client and the delta clients. Keep Slint out of this
//! crate so a CLI can reuse it.
pub mod accounts;
pub mod db;
pub mod debug;
pub mod decide;
pub mod delta;
pub mod engine;
pub mod files;
pub mod filestate;
pub mod ignore;
pub mod rclone;
pub mod runner;
pub mod settings;
pub mod trash;
pub mod web;
