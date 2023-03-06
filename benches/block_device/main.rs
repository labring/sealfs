//! run the benchmark with:
//!     cargo bench --bench block_device --features=disk-db

use criterion::{criterion_group, criterion_main, Criterion};
use sealfs::server::storage_engine::{block_device::block_engine::BlockEngine, StorageEngine};

fn write_file(engine: &BlockEngine, n: isize) {
    (0..n).for_each(|_| {
        let bytes = vec![1u8; 10240];
        engine
            .write_file("test".to_string(), bytes.as_slice(), 0)
            .unwrap();
    })
}

fn read_file(engine: &BlockEngine, n: isize) {
    (0..n * 10).for_each(|_| {
        engine.read_file("test".to_string(), 10240, 0).unwrap();
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    // You should replace with your raw device.
    let engine = BlockEngine::new("", "/dev/sda14");

    c.bench_function("block device test", |b| {
        b.iter(|| {
            write_file(&engine, 512);
            read_file(&engine, 512);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
