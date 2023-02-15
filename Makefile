target_dir = target/debug
deps = 	protobuf-compiler	\
		libfuse-dev			\
		libcapstone-dev		

all: install_deps build test

install_deps:
	sudo apt-get install $(deps) -y

build: 
	cargo build --all --verbose --release

test:
	cargo test
	./servers_run.sh &
	./client_run.sh
