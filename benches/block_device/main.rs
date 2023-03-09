//! run the benchmark with:
//!     cargo bench --bench block_device --features=disk-db

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use sealfs::server::storage_engine::{block_engine::BlockEngine, meta_engine, StorageEngine};
use std::process::Command;

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
    Command::new("bash")
        .arg("-c")
        .arg("dd if=/dev/zero of=node1 bs=4M count=1")
        .output()
        .unwrap();
    Command::new("bash")
        .arg("-c")
        .arg("losetup /dev/loop8 node1")
        .output()
        .unwrap();
    let meta_engine = Arc::new(meta_engine::MetaEngine::new("/tmp/bench/db"));
    let engine = BlockEngine::new("/dev/loop8", meta_engine);

    c.bench_function("block device test", |b| {
        b.iter(|| {
            write_file(&engine, 512);
            read_file(&engine, 512);
        })
    });
    Command::new("bash")
        .arg("-c")
        .arg("losetup -d /dev/loop8")
        .output()
        .unwrap();
    Command::new("bash")
        .arg("-c")
        .arg("rm node1")
        .output()
        .unwrap();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
