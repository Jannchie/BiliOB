#!/bin/bash
tasks=(start_scheduler.py)

for var in ${tasks[@]} 
do 
    ps -ef | grep $var | grep -v grep | cut -c 9-15 | xargs kill -9 
    nohup python $var 1>log.log 2>&1 &
done 
