
for ((l=1; l<=100; l++))
do 
    for ((i=1; i<=10; i++))
    do
        mkdir ~/fs/test_rm$i
        echo "test" >> ~/fs/test_rm$i/test.log
        sleep 0.1
    done

    for ((i=1; i<=10; i++))
    do
        cat ~/fs/test_rm$i/test.log
        echo "test" >> ~/fs/test_rm$i/test.log
        sleep 0.1
    done

    for ((i=1; i<=10; i++))
    do
        sleep 0.1
    done

    for ((i=1; i<=10; i++))
    do
        rm ~/fs/test_rm$i/test.log
        echo heart
        rm -r ~/fs/test_rm$i
        sleep 0.1
    done
done

kill $(jobs -p)