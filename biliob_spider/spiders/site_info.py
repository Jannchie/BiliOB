# coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import SiteItem
import time
import json
import logging
from pymongo import MongoClient
import datetime

from scrapy_redis.spiders import RedisSpider
from biliob_tracer.task import SpiderTask
from db import db


class OnlineSpider(RedisSpider):
    name = "site"
    allowed_domains = ["bilibili.com"]
    start_urls = ['https://api.bilibili.com/x/web-interface/online']
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.SiteInfoPipeline': 300
        }
    }

    def __init__(self):
        self.task = SpiderTask('全站信息爬虫', collection=db['tracer'])

    def parse(self, response):
        try:
            self.task.crawl_count += 1
            r = json.loads(response.body)
            d = r["data"]
            item = SiteItem()
            item['region_count'] = d['region_count']
            item['all_count'] = d['all_count']
            item['web_online'] = d['web_online']
            item['play_online'] = d['play_online']
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
