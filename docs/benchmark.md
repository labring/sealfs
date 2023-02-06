## benchmark

we use library [criterion](https://github.com/bheisler/criterion.rs) for benchmark.

add benchmark like such:

```toml
# Cargo.toml

[[bench]]
name = "rpc"
harness = false
```

create the file benches/rpc/main.rs or benches/rpc.rs.

```rust
// benches/rpc/main.rs or benches/rpc.rs
// ......
use criterion::{criterion_group, criterion_main, Criterion};

fn rpc_benchmark(c: &mut Criterion) {
    // add your bench like below.
    c.bench_function("rpc_bench50000", |b| b.iter(|| cli(50000)));
}

criterion_group!(benches, rpc_benchmark);
criterion_main!(benches);
```

to run benchmark

```shell
cargo bench
```
