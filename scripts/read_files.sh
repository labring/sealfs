
for ((l=1; l<=100; l++))
do 
    for ((i=1; i<=100; i++))
    do
        mkdir ~/fs/test_rm$i
        echo "test" >> ~/fs/test_rm$i/test.log
    done

    for ((i=1; i<=100; i++))
    do
        cat ~/fs/test_rm$i/test.log
        sleep 0.5
    done

    for ((i=1; i<=100; i++))
    do
        echo "test" >> ~/fs/test_rm$i/test.log
        sleep 0.5
    done

    for ((i=1; i<=100; i++))
    do
        rm ~/fs/test_rm$i/test.log
        rm -r ~/fs/test_rm$i
        sleep 0.5
    done
done

kill $(jobs -p)