set +e
# ps and kill the process start by this command "target/debug/server"
ps -ef | grep "target/debug/server" | grep -v grep | awk '{print $2}' | xargs kill -9
# ps and kill the process start by this command "target/debug/manager"
ps -ef | grep "target/debug/manager" | grep -v grep | awk '{print $2}' | xargs kill -9
# ps and kill the process start by this command "target/debug/client"
ps -ef | grep "target/debug/client" | grep -v grep | awk '{print $2}' | xargs kill -9

# ps and kill the process start by this command "target/release/server"
ps -ef | grep "target/release/server" | grep -v grep | awk '{print $2}' | xargs kill -9
# ps and kill the process start by this command "target/release/manager"
ps -ef | grep "target/release/manager" | grep -v grep | awk '{print $2}' | xargs kill -9
# ps and kill the process start by this command "target/release/client"
ps -ef | grep "target/release/client" | grep -v grep | awk '{print $2}' | xargs kill -9

# ps and kill the process start by this command "target/release/client"
ps -ef | grep "run_all" | grep -v grep | awk '{print $2}' | xargs kill -9