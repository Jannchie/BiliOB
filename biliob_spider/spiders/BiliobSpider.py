# coding=utf-8
import datetime
import json
import logging
import time

import redis
import scrapy
from pymongo import MongoClient
from scrapy.http import Request
from scrapy_redis.spiders import RedisSpider

from biliob_spider.items import AuthorItem
from db import redis_connect_string, db
from mail import mailer


class BiliobSpider(scrapy.spiders.Spider):
    name = "BiliobSpider"
    allowed_domains = ["bilibili.com"]
    start_urls = ['www.bilibili.com']
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.BiliobPipeline': 300
        },
        'DOWNLOAD_DELAY': 10
    }
    def __init__(self):

        self.db = db  # 获得数据库的句柄
        self.coll = self.db['author']  # 获得collection的句柄
        self.redis_connection = redis.from_url(redis_connect_string)

    def parse(self, response):
        try:
            j = json.loads(response.body)
            name = j['data']['card']['name']
            mid = j['data']['card']['mid']

            # 刷新redis数据缓存
            self.redis_connection.delete("author_detail::{}".format(mid))

            sex = j['data']['card']['sex']
            face = j['data']['card']['face']
            fans = j['data']['card']['fans']
            attention = j['data']['card']['attention']
            level = j['data']['card']['level_info']['current_level']
            official = j['data']['card']['Official']['title']
            archive = j['data']['archive_count']
            article = j['data']['article_count']
            face = j['data']['card']['face']
            item = AuthorItem()
            item['mid'] = int(mid)
            item['name'] = name
            item['face'] = face
            item['official'] = official
            item['sex'] = sex
            item['level'] = int(level)
            item['data'] = {
                'fans': int(fans),
                'attention': int(attention),
                'archive': int(archive),
                'article': int(article),
                'datetime': datetime.datetime.now()
            }
            item['c_fans'] = int(fans)
            item['c_attention'] = int(attention)
            item['c_archive'] = int(archive)
            item['c_article'] = int(article)
            yield Request(
                "https://api.bilibili.com/x/space/upstat?mid={mid}".format(
                    mid=str(mid)),
                meta={'item': item},
                method='GET',
                callback=self.parse_view)
        except Exception as error:
            # 出现错误时打印错误日志
            mailer.send(
                to=["604264970@qq.com"],
                subject="BiliobSpiderError",
                body="{}\n{}\n{}".format(item, response.url, error),
            )
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)

    def parse_view(self, response):
        j = json.loads(response.body)
        archive_view = j['data']['archive']['view']
        article_view = j['data']['article']['view']
        item = response.meta['item']
        item['data']['archiveView'] = archive_view
        item['data']['articleView'] = article_view
        item['c_archive_view'] = int(archive_view)
        item['c_article_view'] = int(article_view)

        yield item
