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

use crate::server::proto::service_server::ServiceServer;
use crate::server::Server as RpcServer;
use mimalloc::MiMalloc;
use std::error::Error;
use tokio::runtime::Builder;
use tonic::transport::Server;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<(), Box<dyn Error>> {
    let addr = "[::1]:1107".parse().unwrap();
    let cpus = num_cpus::get();

    let rpc_runtime = Builder::new_multi_thread()
        .worker_threads(cpus)
        .enable_io()
        .build()
        .unwrap();

    rpc_runtime.block_on(async move {
        let server = RpcServer::new(cpus, cpus);
        Server::builder()
            .add_service(ServiceServer::new(server))
            .serve(addr)
            .await
    })?;

    return Ok(());
}
