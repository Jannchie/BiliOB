#!/usr/bin/python3.6
# -*- coding:utf-8 -*-

import schedule
import time
from subprocess import Popen
import logging
import threading
from db import redis_connection

import requests
import redis
from lxml import etree
import json

VIDEO_URL = "https://api.bilibili.com/x/article/archives?ids={aid}"
VIDEO_KEY = "videoRedis:start_urls"
AUTHOR_URL = "https://api.bilibili.com/x/web-interface/card?mid={mid}"
AUTHOR_KEY = "authorRedis:start_urls"
DANMAKU_FROM_AID_URL = "https://api.bilibili.com/x/web-interface/view?aid={aid}"
DANMAKU_KEY = "DanmakuAggregate:start_urls"


def sendAuthorCrawlRequest(mid):
    redis_connection.rpush(AUTHOR_KEY, AUTHOR_URL.format(mid=mid))


def sendVideoCrawlRequest(aid):
    redis_connection.rpush(VIDEO_KEY, VIDEO_URL.format(aid=aid))


def priorityAuthorCrawlRequest(mid):
    redis_connection.lpush(AUTHOR_KEY, AUTHOR_URL.format(mid=mid))


def priorityVideoCrawlRequest(aid):
    redis_connection.lpush(VIDEO_KEY, VIDEO_URL.format(aid=aid))


def crawlOnlineTopListData():
    ONLINE_URL = 'https://www.bilibili.com/video/online.html'
    response = requests.get(ONLINE_URL)
    data_text = etree.HTML(response.content.decode(
        'utf8')).xpath('//script/text()')[-2]
    j = json.loads(data_text.lstrip('window.__INITIAL_STATE__=')[:-122])
    for each_video in j['onlineList']:
        aid = each_video['aid']
        mid = each_video['owner']['mid']
        if mid not in [7584632, 928123]:
            priorityAuthorCrawlRequest(mid)
        priorityVideoCrawlRequest(aid)
        print(aid)
        print(mid)
    pass


def site():
    Popen(["scrapy", "crawl", "site"])


def bangumi():
    Popen(["scrapy", "crawl", "bangumi"])


def donghua():
    Popen(["scrapy", "crawl", "donghua"])


def update_author():
    Popen(["scrapy", "crawl", "authorUpdate"])


def auto_add_author():
    Popen(["scrapy", "crawl", "authorAutoAdd"])


def video_watcher():
    Popen(["scrapy", "crawl", "videoWatcher"])


def video_spider():
    Popen(["scrapy", "crawl", "videoSpider"])


def video_spider_all():
    Popen(["scrapy", "crawl", "videoSpiderAll"])


def online():
    Popen(['scrapy', 'crawl', 'online'])


def strong():
    crawlOnlineTopListData()


def data_analyze():
    Popen(['python', 'run_analyzer.py'])


# def weekly_analyze():
#     Popen(['python', 'run_weekly_analyzer.py'])


# def bili_monthly_rank():
#     Popen(['scrapy', 'crawl', 'biliMonthlyRank'])


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


schedule.every().day.at('11:40').do(run_threaded, data_analyze)

print('开始运行计划任务..')
while True:
    schedule.run_pending()
    time.sleep(60)
