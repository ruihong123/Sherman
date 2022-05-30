#!/bin/bash

addr=$(head -1 ../memcached.conf)
port=$(awk 'NR==2{print}' ../memcached.conf)

## kill old me
#ssh ${addr} "cat /tmp/memcached.pid | xargs kill"
#
## launch memcached
#ssh -o StrictHostKeyChecking=no ${addr} "memcached -u root -l ${addr} -p  ${port} -c 10000 -d -P /tmp/memcached.pid"
#sleep 1

# init 
echo -e "set ComputeNum 0 0 1\r\n0\r\nquit\r" | nc ${addr} ${port}
echo -e "set MemoryNum 0 0 1\r\n0\r\nquit\r" | nc ${addr} ${port}
echo -e "delete 0M-0C\r\nquit\r" | nc ${addr} ${port}
echo -e "delete 0C-0M\r\nquit\r" | nc ${addr} ${port}
