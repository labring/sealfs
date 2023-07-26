FROM debian:bullseye-20221205

RUN apt update && apt upgrade -y && apt-mark unhold libcap2 && \
    apt install -y fuse3 libfuse3-3 libfuse2 libibverbs1 && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY target/debug/client /usr/local/bin/client

ENTRYPOINT ["/usr/local/bin/client"]
