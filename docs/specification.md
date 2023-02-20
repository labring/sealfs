# specifications

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
    c.bench_function("rpc_bench100000", |b| b.iter(|| cli(100000)));
}

//define benchmark configuration like below.
criterion_group!(
    name=benches;
    config=Criterion::default().significance_level(0.1).sample_size(10);
    targets = rpc_benchmark
);
criterion_main!(benches);
```

to run benchmark
`cargo bench --bench <bench-name>`

```shell
cargo bench --bench rpc
```

## log

We use library [env-logger](https://docs.rs/env_logger/0.10.0/env_logger/) including five log level: "ERROR", "WARN", "INFO", "DEBUG", "TRACE".

For flexible usage, you can specify the log level by `./target/debug/server --log-level info`. The default log level is set in `examples/*.yaml`

Logging principles:

1. Logging key change information
2. Applying the right level of logging;
3. Avoid duplication of logging information;
4. ......

Details about log level :

**error** : Designates very serious errors.

error log generally refers to program-level errors or serious business errors that do not affect the operation of the program.

**warn** : Designates hazardous situations.

warn log implies that needs attention, but not sure if an error occurred. For example, a user connection is closed abnormally, the relevant configuration cannot be found and only the default configuration can be used, retry after XX seconds, etc.

**info** : Designates useful information.

info log often used to record information about the operation of a program, such as user operations or changes in status, connection establishment and termination.

**debug** : Designates lower priority information.

debug log always used for detailed information, such as user request details tracking, configuration information read.

**trace** : Designates very low priority, often extremely verbose, information.
