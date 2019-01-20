#coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import TagItem
import time
import json
import logging
from pymongo import MongoClient
import datetime


class TagSpider(scrapy.spiders.Spider):
    name = "tag"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.TagPipeLine': 300
        },
        'DOWNLOAD_DELAY': 1
    }
    def start_requests(self):
        for i in range(0,9999999):
            url = 'https://api.bilibili.com/x/tag/info?tag_id={tag_id}'.format(tag_id=i)
            yield Request(url)
    def parse(self, response):
        try:
            r = json.loads(response.body)
            d = r["data"]
            item = TagItem()
            item['tag_id'] = d['tag_id']
            item['tag_name'] = d['tag_name']
            item['ctime'] = d['ctime']
            item['use'] = d['count']['use']
            item['atten'] = d['count']['atten']
            yield item

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
