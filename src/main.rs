#![feature(box_patterns)]
#![feature(div_duration)]
#![feature(duration_constants)]
#![allow(clippy::needless_return)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::future_not_send
)]
mod server;
mod storage;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    println!("Hello, world!");
}
