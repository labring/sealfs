for ((i=1; i<=10; i++))
do
    mkdir ~/fs/test_rm$i
    echo "test" >> ~/fs/test_rm$i/test.log
    sleep 0.1
done

target/debug/client --log-level info add 127.0.0.1:8090
./target/debug/server --server-address 127.0.0.1:8090 --database-path /data/database5/ --storage-path /data/storage5/ --log-level info


for ((i=1; i<=10; i++))
do
    rm ~/fs/test_rm$i/test.log
    echo heart
    rm -r ~/fs/test_rm$i
    sleep 0.1
done