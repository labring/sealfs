mod client;
mod server;

use client::gcli;
use criterion::{criterion_group, criterion_main, Criterion};
use server::server;

extern crate core_affinity;

fn grpc_benchmark(c: &mut Criterion) {
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
    // c.bench_function("grpc_bench0", |b| b.iter(|| gcli(0)));
    // c.bench_function("grpc_bench10", |b| b.iter(|| gcli(10)));
    // c.bench_function("grpc_bench100", |b| b.iter(|| gcli(100)));
    // c.bench_function("grpc_bench1000", |b| b.iter(|| gcli(1000)));
    // c.bench_function("grpc_bench10000", |b| b.iter(|| gcli(10000)));
    c.bench_function("grpc_bench100000", |b| b.iter(|| gcli(100000)));
}

criterion_group!(
    name=benches;
    config=Criterion::default().significance_level(0.1).sample_size(10);
    targets = grpc_benchmark
);
criterion_main!(benches);
