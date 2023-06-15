
s=(SyncNewHashRing PreTransfer Transferring PreFinish Finishing Idle)

for i in {0..5}
do 
    echo "test $i"
    echo ""

    scripts/close_all_instances.sh
    scripts/run_all.sh /data warn&
    sleep 10

    mkdir ~/fs/test_rm5

    target/debug/client --log-level info delete 127.0.0.1:8089

    while true
    do
        status=`target/debug/client status`
        #echo $status+${s[$i]}
        if [[ $status == *${s[$i]}* ]];
        then
            break
        fi
        sleep 0.1
    done

    echo "test" >> ~/fs/test_rm5/test.log

    rm ~/fs/test_rm5/test.log

    while true
    do
        status=`target/debug/client status`
        #echo $status+${s[$i]}
        if [[ $status == *${s[5]}* ]];
        then
            break
        fi
        sleep 0.1
    done

    rm -r ~/fs/test_rm5

    kill $(jobs -p)
done