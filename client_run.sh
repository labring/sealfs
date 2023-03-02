#!/bin/bash

function finish() {
    sudo umount ~/fs
    sudo rm -rf ~/fs
    trap 'kill $(jobs -p)' EXIT
    exit $1
}

function green_font() {
    echo -e "\033[32m$1\033[0m\c"
}

echo "start fuse_client_run"
sleep 3
mkdir ~/fs
./target/debug/client ~/fs &
sleep 3
start_time=$[$(date +%s%N)/1000000]
for ((i=0; i<10; i++))
do
    mkdir ~/fs/dir_${i}
    for ((j=0; j<10; j++))
    do
        echo "test_${i}" >> ~/fs/dir_${i}/${i}_${j}.log
    done

    echo -e "$(green_font dir_${i}): $(ls ~/fs/dir_${i})"

    for ((j=0; j<10; j++))
    do
        if [ -z $(ls ~/fs/dir_${i} | grep ${i}_${j}.log) ]
        then
            echo "fail: don't find ~/fs/dir_${i}/${i}_${j}.log"
            finish 1
        fi

        if [ -z $(cat ~/fs/dir_${i}/${i}_${j}.log | grep test_${i}) ]
        then
            echo "fail: ~/fs/dir_${i}/${i}_${j}.log inconsistent content"
            finish 1
        fi
    done

    for ((j=0; j<10; j++))
    do
        rm ~/fs/dir_${i}/${i}_${j}.log
    done

    for ((j=0; j<10; j++))
    do
        if [ ! -z $(ls ~/fs/dir_${i} | grep ${i}_${j}.log) ]
        then
            echo "fail: rm ~/fs/dir_${i}/${i}_${j}.log fail"
            finish 1
        fi
    done
done
end_time=$[$(date +%s%N)/1000000]
result=$[ $end_time - $start_time ]
echo -e "all tests ok, cost: $(green_font ${result}ms)"
finish 0