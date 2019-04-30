#!/bin/bash
spiders=(authorRedis videoRedis bangumiAndDonghua authorAutoAdd videoAutoAdd site online)

for var in ${spiders[@]} 
do 
    ps -ef | grep $var | grep -v grep | cut -c 9-15 | xargs kill -9 
    nohup scrapy crawl $var 1>log.log 2>&1 &
done 


ps -ef | grep DanmakuAggregate | grep -v grep | cut -c 9-15 | xargs kill -9 
cd danmaku_spider/ && nohup scrapy crawl DanmakuAggregate 1>log.log 2>&1 &
ps -ef |grep py
