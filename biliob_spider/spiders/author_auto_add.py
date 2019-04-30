# coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import AuthorItem
import time
import json
import logging
from pymongo import MongoClient
import datetime
from scrapy_redis.spiders import RedisSpider
from biliob_tracer.task import ExistsTask
from db import db


class AuthorAutoAddSpider(RedisSpider):
    name = "authorAutoAdd"
    allowed_domains = ["bilibili.com"]

    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.AuthorPipeline': 300
        },
        'DOWNLOAD_DELAY': 10
    }

    def __init__(self):
        ExistsTask('活跃作者自动追加爬虫', collection=db['tracer'])

    def parse(self, response):
        try:
            url_list = response.xpath(
                "//*[@id='app']/div[2]/div/div[1]/div[2]/div[3]/ul/li/div[2]/div[2]/div/a/@href"
            ).extract()

            # 为了爬取分区、粉丝数等数据，需要进入每一个视频的详情页面进行抓取
            for each_url in url_list:
                yield Request(
                    "https://api.bilibili.com/x/web-interface/card?mid=" +
                    each_url[21:],
                    method='GET',
                    callback=self.detailParse)
        except Exception as error:
            # 出现错误时打印错误日志
            mailer.send(
                to=["604264970@qq.com"],
                subject="BiliobSpiderError",
                body="{}\n{}".format(response.url, error),
            )
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)

    def detailParse(self, response):
        try:
            j = json.loads(response.body)
            name = j['data']['card']['name']
            mid = j['data']['card']['mid']
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

            # 粉丝数大于1000才加入
            if int(fans) > 1000:
                item['c_fans'] = int(fans)
                item['c_attention'] = int(attention)
                item['c_archive'] = int(archive)
                item['c_article'] = int(article)
                item['mid'] = int(mid)
                item['name'] = name
                item['face'] = face
                item['official'] = official
                item['sex'] = sex
                item['focus'] = True
                item['level'] = int(level)
                item['data'] = {
                    'fans': int(fans),
                    'attention': int(attention),
                    'archive': int(archive),
                    'article': int(article),
                    'datetime': datetime.datetime.now()
                }
                yield item
        except Exception as error:
            # 出现错误时打印错误日志
            mailer.send(
                to=["604264970@qq.com"],
                subject="BiliobSpiderError",
                body="{}\n{}".format(response.url, error),
            )
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)
