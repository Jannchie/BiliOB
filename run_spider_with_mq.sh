source ~/biliob-spider-env/bin/activate
ps -ef | grep authorRedis | grep -v grep | cut -c 9-15 | xargs kill -9
ps -ef | grep videoRedis | grep -v grep | cut -c 9-15 | xargs kill -9
ps -ef | grep DanmakuAggregate | grep -v grep | cut -c 9-15 | xargs kill -9

nohup scrapy crawl authorRedis 1>log.log 2>&1 &
nohup scrapy crawl videoRedis 1>log.log 2>&1 &

cd danmaku_spider/ && nohup scrapy crawl DanmakuAggregate 1>log.log 2>&1 &
ps -ef |grep py