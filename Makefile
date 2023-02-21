target_dir = target/debug
features := disk-db
flags += --workspace --verbose --features=$(features)
deps = 	protobuf-compiler	\
		libfuse-dev			\
		libcapstone-dev		

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
	./servers_run.sh &
	./client_run.sh
