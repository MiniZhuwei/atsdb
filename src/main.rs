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
mod storage;

fn main() {
    println!("Hello, world!");
}
