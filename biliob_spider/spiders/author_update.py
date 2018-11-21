#coding=utf-8
import scrapy
from scrapy.http import Request
from biliob_spider.items import AuthorItem
import time
import json
import logging
from pymongo import MongoClient
import datetime
from db import settings


class AuthorUpdate(scrapy.spiders.Spider):
    name = "authorUpdate"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.AuthorPipeline': 300
        },
        'DOWNLOAD_DELAY': 1
    }

    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['author']  # 获得collection的句柄

    def start_requests(self):
        c = self.coll.find({'$or':[{'focus':True},{'forceFocus':True}]}, {"mid": 1})
        for each_doc in c:
            yield Request(
                "https://api.bilibili.com/x/web-interface/card?mid=" + str(
                    each_doc['mid']),
                method='GET')

    def parse(self, response):
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
            yield Request("http://api.bilibili.com/x/space/upstat?mid={mid}".format(mid=str(mid)),meta={'item': item},method='GET',callback=self.parse_view)
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)
    
    def parse_view(self,response):
        j = json.loads(response.body)
        archiveView = j['data']['archive']['view']
        articleView = j['data']['article']['view']
        item = response.meta['item']
        item['data']['archiveView'] = archiveView
        item['data']['articleView'] = articleView
        yield item
