#!/bin/bash

function finish() {
    sudo umount ~/fs
    sudo rm -rf ~/fs
    trap 'kill $(jobs -p)' EXIT
    exit $1
}

trap 'onCtrlC' INT
function onCtrlC () {
    finish 0
}

function green_font() {
    echo -e "\033[32m$1\033[0m\c"
}

echo "start fuse_client_run"

# exit with 1 if no argument
if [ $# -eq 0 ]
then
    echo "no argument"
    exit 1
fi

set +e

sudo umount ~/fs
mkdir -p ~/fs

set -e

# check if $2 is empty, if empty, let $log_level = warn, else $log_level = $2
if [ -z $2 ]; then
    log_level=warn
else
    log_level=$2
fi

SEALFS_CONFIG_PATH=./examples ./target/debug/manager --log-level $log_level &

sudo rm -rf $1/database*
sudo rm -rf $1/storage*
for ((i=0; i<5; i++))
do
    port=$[8085+$i]
    SEALFS_CONFIG_PATH=./examples ./target/debug/server --server-address 127.0.0.1:${port} --database-path $1/database${i}/ --storage-path $1/storage${i}/ --log-level $log_level &
done

sleep 3

./target/debug/client --log-level $log_level create test1 100000 &

sleep 3

./target/debug/client --log-level $log_level mount ~/fs test1 &
sleep 3

echo "press ctrl+c to stop"
sleep 100000