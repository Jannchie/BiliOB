# coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import VideoOnline
import time
import json
import logging
from pymongo import MongoClient
import datetime
from scrapy_redis.spiders import RedisSpider
from biliob_tracer.task import SpiderTask
from db import db


class OnlineSpider(RedisSpider):
    name = "online"
    allowed_domains = ["bilibili.com"]
    start_urls = ['https://www.bilibili.com/video/online.html']
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.OnlinePipeline': 300
        }
    }

    def __init__(self):
        self.task = SpiderTask('同时在线人数爬虫', collection=db['tracer'])

    def parse(self, response):
        try:
            self.task.crawl_count += 1
            video_list = response.xpath('//*[@id="app"]/div[2]/div[2]/div')

            # 为了爬取分区、粉丝数等数据，需要进入每一个视频的详情页面进行抓取
            title_list = video_list.xpath('./a/p/text()').extract()
            watch_list = video_list.xpath('./p/b/text()').extract()
            author_list = video_list.xpath('./div[1]/a/text()').extract()
            href_list = video_list.xpath('./a/@href').extract()
            for i in range(len(title_list)):
                item = VideoOnline()
                item['title'] = title_list[i]
                item['author'] = author_list[i]
                item['data'] = {
                    'datetime': datetime.datetime.now(),
                    'number': watch_list[i]
                }
                item['aid'] = href_list[i][9:-1]
                # 为了爬取分区等数据，需要进入每一个视频的详情页面进行抓取
                yield Request(
                    "https://www.bilibili.com" + href_list[i],
                    meta={'item': item},
                    callback=self.detailParse)
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

    def detailParse(self, response):
        try:
            item = response.meta['item']
            c = response.xpath("//span[@class='crumb'][2]/a/text()").extract()
            if c != []:
                item['channel'] = response.xpath(
                    "//span[@class='crumb'][2]/a/text()").extract()[0]
            else:
                item['channel'] = '番剧'

            c = response.xpath("//span[@class='crumb'][3]/a/text()").extract()
            if c != []:
                item['subChannel'] = response.xpath(
                    "//span[@class='crumb'][3]/a/text()").extract()[0]
            else:
                item['subChannel'] = '番剧'

            yield item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error("视频爬虫在解析细节时发生错误")
            logging.error(response.url)
            logging.error(error)
