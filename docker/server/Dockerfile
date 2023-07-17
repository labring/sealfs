FROM debian:bullseye-20221205

RUN apt update && apt upgrade -y && apt-mark unhold libcap2 && \
    apt install -y libfuse3-3 libfuse2 libibverbs1 && \
    apt clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY target/debug/server /usr/local/bin/server

ENTRYPOINT ["/usr/local/bin/server"]
