# coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import VideoWatcherItem
import time
import json
import logging
from pymongo import MongoClient
import datetime
from db import settings
from scrapy_redis.spiders import RedisSpider

from biliob_tracer.task import SpiderTask


class VideoAutoAddSpider(RedisSpider):
    name = "videoAutoAdd"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.VideoAddPipeline': 300,
            'biliob_spider.pipelines.AuthorChannelPipeline': 301
        },
        'DOWNLOAD_DELAY': 0.5
    }

    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['author']  # 获得collection的句柄
        self.task = SpiderTask('观测UP主的视频数据自动追加爬虫', collection=self.db['tracer'])

    def parse(self, response):
        try:
            self.task.crawl_count += 1
            j = json.loads(response.body)
            if len(j['data']['vlist']) == 0:
                return
            channels = j['data']['tlist']
            list_channel = []
            for each_channel in channels:
                list_channel.append(channels[each_channel])
            aid = []
            for each in j['data']['vlist']:
                aid.append(int(each['aid']))
                mid = each['mid']
            item = VideoWatcherItem()
            item['aid'] = aid
            item['channels'] = list_channel
            item['mid'] = mid
            yield item
        except Exception as error:
            # 出现错误时打印错误日志
            self.task.crawl_failed += 1
            mailer.send(
                to=["604264970@qq.com"],
                subject="BiliobSpiderError",
                body="{}\n{}\n{}".format(item, response.url, error),
            )
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)
