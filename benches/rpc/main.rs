//! run the benchmark with:
//!     cargo bench --bench rpc

#![allow(unused)]
mod client;
mod server;
use client::{cli, cli_size};
use criterion::{criterion_group, criterion_main, Criterion};

use server::server;
fn rpc_benchmark(c: &mut Criterion) {
    std::thread::spawn(|| {
        let core_ids = core_affinity::get_core_ids().unwrap();
        let core_id0 = core_ids[0];
        core_affinity::set_for_current(core_id0);
        match server() {
            Ok(()) => println!("server start succeed!"),
            Err(_) => println!("server start failed!"),
        };
    });
    // wait for server to start
    std::thread::sleep(std::time::Duration::from_secs(5));
    // c.bench_function("rpc_bench0", |b| b.iter(|| cli(0)));
    // c.bench_function("rpc_bench10", |b| b.iter(|| cli(10)));
    // c.bench_function("rpc_bench100", |b| b.iter(|| cli(100)));
    // c.bench_function("rpc_bench1000", |b| b.iter(|| cli(1000)));
    c.bench_function("rpc_bench100000", |b| b.iter(|| cli(100000)));
    // c.bench_function("rpc_bench100000_without_data", |b| b.iter(|| cli(100000)));
    // c.bench_function("rpc_bench100000_data_size_1024", |b| {
    //     b.iter(|| cli_size(100000, 1024))
    // });
    // c.bench_function("rpc_bench100000_data_size_1024_4", |b| {
    //     b.iter(|| cli_size(100000, 1024 * 4))
    // });
    // c.bench_function("rpc_bench100000_data_size_1024_16", |b| {
    //     b.iter(|| cli_size(100000, 1024 * 16))
    // });
    // c.bench_function("rpc_bench100000_data_size_1024_64", |b| {
    //     b.iter(|| cli_size(100000, 1024 * 64))
    // });
    // c.bench_function("rpc_bench100000_data_size_1024_256", |b| {
    //     b.iter(|| cli_size(100000,1024*256))
    // });
    // c.bench_function("rpc_bench100000_data_size_1024_1024", |b| {
    //     b.iter(|| cli_size(100000,1024*1024))
    // });
}

criterion_group!(
    name=benches;
    config=Criterion::default().significance_level(0.1).sample_size(10);
    targets = rpc_benchmark
);
criterion_main!(benches);
