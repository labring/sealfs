target_dir = target/debug
features := disk-db
flags += --workspace --verbose --features=$(features)
deps = 	pkg-config protobuf-compiler clang libfuse-dev libcapstone-dev 	\
		iproute2 perftest build-essential net-tools cmake pandoc 		\
		libnl-3-dev libnl-route-3-dev libibverbs-dev

all_release: install_deps release
all_debug: install_deps debug

install_deps:
	sudo apt update && sudo apt-get install $(deps) -y

release: 
	cargo build $(flags) --release

build:
	cargo build $(flags)

test:
	cargo test --features=$(features)

images: manager-image server-image client-image

manager-image:
	docker build -t manager -f docker/manager/Dockerfile . --no-cache

server-image:
	docker build -t server -f docker/server/Dockerfile .

client-image:
	docker build -t client -f docker/client/Dockerfile .