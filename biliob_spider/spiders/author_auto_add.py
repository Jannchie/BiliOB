#coding=utf-8
import scrapy
from scrapy.http import Request
from biliob_spider.items import AuthorItem
import time
import json
import logging
from pymongo import MongoClient
import datetime

class AuthorAutoAddSpider(scrapy.spiders.Spider):
    name = "authorAutoAdd"
    allowed_domains = ["bilibili.com"]
    start_urls = ['https://www.bilibili.com/ranking']
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.AuthorPipeline': 300
        },
        'DOWNLOAD_DELAY' : 10
    }

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
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)

    def detailParse(self, response):
        j = json.loads(response.body)
        name = j['data']['card']['name']
        mid = j['data']['card']['mid']
        sex = j['data']['card']['sex']
        face = j['data']['card']['face']
        fans = j['data']['card']['fans']
        attention = j['data']['card']['attention']
        level = j['data']['card']['level_info']['current_level']
        official = j['data']['card']['Official']['title']
        archive = j['data']['archive']
        article = j['data']['article']
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
        yield item
