#!/bin/bash

echo "start fuse_servers_run"
for ((i=0; i<5; i++))
do
    port=$[8085+$i]
    rm -rf /tmp/database${i}/
    rm -rf /tmp/storage${i}/
    ./target/debug/server --server-address 127.0.0.1:${port} --database-path /tmp/database${i}/ --storage-path /tmp/storage${i}/ &
done

sleep 25
trap 'kill $(jobs -p)' EXIT
echo "fuse_servers_run finish"