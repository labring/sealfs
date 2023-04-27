target_dir = target/debug
features := disk-db
flags += --workspace --verbose --features=$(features)
deps = 	protobuf-compiler	\
		libfuse-dev			\
		libcapstone-dev		\
		iproute2 perftest build-essential net-tools \
		cython pandoc libnl-3-dev libnl-route-3-dev

all_release: install_deps release
all_debug: install_deps debug

install_deps:
	sudo apt-get install $(deps) -y

release: 
	cargo build $(flags) --release

build:
	cargo build $(flags)

test:
	cargo test --features=$(features)
