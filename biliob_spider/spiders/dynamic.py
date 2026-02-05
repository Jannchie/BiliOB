#coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import SiteItem
import time
import json
import logging
from pymongo import MongoClient
import datetime


class DynamicSpider(scrapy.spiders.Spider):
    name = "dynamic"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
        }
    }

    def start_requests(self):
        yield Request(
            "https://api.vc.bilibili.com/dynamic_svr/v1/dynamic_svr/space_history?host_uid=221648",
            method='GET',
            callback=self.parse)

    def parse(self, response):
        try:
            j = json.loads(response.body)
            cards  = j['data']['cards']
            for each_card in cards:

                print('点赞数：{}'.format(each_card['desc']['like']))
                print('UP主ID：{}'.format(each_card['desc']['uid']))
                card = json.loads(each_card['card'])
                if('title' in card):
                    print('标题：{}'.format(card['title']))
                if('description' in card):
                    print('内容：{}'.format(card['description']))

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
