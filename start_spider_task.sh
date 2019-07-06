#!/bin/bash
<<<<<<< HEAD
spiders=(authorRedis videoRedis bangumiAndDonghua authorAutoAdd videoAutoAdd site online tagAdder)
=======
spiders=(authorRedis videoRedis bangumiAndDonghua authorAutoAdd videoAutoAdd site online)
>>>>>>> ccbc48ffa2e158f353a8174aa02f6160e68a8575

for var in ${spiders[@]} 
do 
    ps -ef | grep $var | grep -v grep | cut -c 9-15 | xargs kill -9 
    nohup scrapy crawl $var 1>log.log 2>&1 &
done 


ps -ef | grep DanmakuAggregate | grep -v grep | cut -c 9-15 | xargs kill -9 
cd danmaku_spider/ && nohup scrapy crawl DanmakuAggregate 1>log.log 2>&1 &
ps -ef |grep py
