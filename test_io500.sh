#!/bin/bash

function finish() {
    sudo umount ~/fs
    sudo rm -rf ~/fs
    rm -rf io500
    trap 'kill $(jobs -p)' EXIT
    exit $1
}

function green_font() {
    echo -e "\033[32m$1\033[0m\c"
}

echo "start fuse_client_run"

set +e

sudo umount ~/fs
mkdir -p ~/fs

set -e

sudo rm -rf $1/database*
sudo rm -rf $1/storage*
for ((i=0; i<5; i++))
do
    port=$[8085+$i]
    SEALFS_CONFIG_PATH=./examples ./target/debug/server --server-address 127.0.0.1:${port} --database-path $1/database${i}/ --storage-path $1/storage${i}/ --log-level warn &
done

sleep 3

SEALFS_CONFIG_PATH=./examples ./target/debug/client ~/fs --log-level warn &
sleep 3
start_time=$[$(date +%s%N)/1000000]

SELF_HOSTED=1 

if [ $SELF_HOSTED -eq 1 ]
then
    cp -r ~/io500/io500 .
    cd io500
else
    git clone https://github.com/IO500/io500.git
    cd io500
    ./prepare.sh
fi

echo "[global]" > config-minimal.ini
echo "datadir = /home/luan/fs" >> config-minimal.ini
echo "" >> config-minimal.ini
echo "[debug]" >> config-minimal.ini
echo "stonewall-time = 5" >> config-minimal.ini

timeout -s SIGKILL 200 mpirun -np 2 ./io500 config-minimal.ini

cd ..

end_time=$[$(date +%s%N)/1000000]
result=$[ $end_time - $start_time ]
echo -e "all tests ok, cost: $(green_font ${result}ms)"
finish 0