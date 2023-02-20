use criterion::{criterion_group, criterion_main, Criterion};
use nix::sys::stat::Mode;
use rand::prelude::*;
use sealfs::server::storage_engine::{
    default_engine::{self, DefaultEngine},
    StorageEngine,
};

fn create_file(engine: &DefaultEngine, n: isize) {
    let mode = Mode::S_IRUSR
        | Mode::S_IWUSR
        | Mode::S_IRGRP
        | Mode::S_IWGRP
        | Mode::S_IROTH
        | Mode::S_IWOTH;
    (0..n).for_each(|i| {
        println!("create {}", i);
        engine.create_file(i.to_string(), mode).unwrap();
    })
}

fn delete_file(engine: &DefaultEngine, n: isize) {
    (0..n).for_each(|i| {
        println!("delete {}", i);
        engine.delete_file(i.to_string()).unwrap();
    })
}

fn write_file(engine: &DefaultEngine, n: isize) {
    (0..n).for_each(|_| {
        let mut rng = rand::thread_rng();
        let i: usize = rng.gen::<usize>() % n as usize;
        println!("write {}", i);
        engine.write_file(i.to_string(), b"hello world", 0).unwrap();
    })
}

fn read_file(engine: &DefaultEngine, n: isize) {
    (0..n).for_each(|_| {
        let mut rng = rand::thread_rng();
        let i: usize = rng.gen::<usize>() % n as usize;
        let data = engine.read_file(i.to_string(), 11, 0).unwrap();
        println!("read {:?}", data.to_ascii_lowercase());
    })
}

fn criterion_benchmark(c: &mut Criterion) {
    let engine = default_engine::DefaultEngine::new("/tmp/bench/db", "/tmp/bench/root");

    c.bench_function("default engine file 100", |b| {
        b.iter(|| {
            create_file(&engine, 100);
            write_file(&engine, 100);
            read_file(&engine, 100);
            // delete_file(&engine, 10);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
