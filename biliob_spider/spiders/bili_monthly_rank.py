# coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import RankItem
import time
import json
import logging
from pymongo import MongoClient
import datetime
<<<<<<< HEAD
from scrapy_redis.spiders import RedisSpider


class BiliMonthlyRankSpider(RedisSpider):
    name = "biliMonthlyRank"
    allowed_domains = ["bilibili.com"]
    start_urls = [

=======


class BiliMonthlyRankSpider(scrapy.spiders.Spider):
    name = "biliMonthlyRank"
    allowed_domains = ["bilibili.com"]
    start_urls = [
        'https://www.bilibili.com/ranking/all/1/0/30',
        'https://www.bilibili.com/ranking/all/168/0/30',
        'https://www.bilibili.com/ranking/all/3/0/30',
        'https://www.bilibili.com/ranking/all/129/0/30',
        'https://www.bilibili.com/ranking/all/4/0/30',
        'https://www.bilibili.com/ranking/all/36/0/30',
        'https://www.bilibili.com/ranking/all/160/0/30',
        'https://www.bilibili.com/ranking/all/119/0/30',
        'https://www.bilibili.com/ranking/all/155/0/30',
        'https://www.bilibili.com/ranking/all/5/0/30',
        'https://www.bilibili.com/ranking/all/181/0/30'
>>>>>>> ccbc48ffa2e158f353a8174aa02f6160e68a8575
    ]

    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.BiliMonthlyRankPipeline': 300
        },
    }

    def parse(self, response):
        try:
            url_list = response.xpath(
                '//*[@id="app"]/div[2]/div/div/div[2]/div[3]/ul/li/div[2]/div[2]/a/@href').extract()
            pts_list = response.xpath(
                '//*[@id="app"]/div[2]/div/div/div[2]/div[3]/ul/li/div[2]/div[2]/div[2]/div/text()').extract()
            mid_list = response.xpath(
                '//*[@id="app"]/div[2]/div/div/div[2]/div[3]/ul/li/div[2]/div[2]/div[1]/a/@href').extract()

            title_list = response.xpath(
                '//*[@id="app"]/div[2]/div/div/div[2]/div[3]/ul/li/div[2]/div[2]/a/text()').extract()
            author_list = response.xpath(
                '//*[@id="app"]/div[2]/div/div/div[2]/div[3]/ul/li/div[2]/div[2]/div[1]/a/span/text()').extract()
            aid_list = list(map(lambda x: int(x[27:-1]), url_list))
            pts_list = list(map(lambda x: int(x), pts_list))
            mid_list = list(
                map(lambda x: int(x.lstrip('//space.bilibili.com/').rstrip('/')), mid_list))
            channel = response.xpath(
                "//li[@class='active']/text()").extract()[0]
            # 为了爬取分区、粉丝数等数据，需要进入每一个视频的详情页面进行抓取
            for each in zip(title_list, author_list, aid_list, pts_list, mid_list):
                item = RankItem()
                item['title'] = each[0]
                item['author'] = each[1]
                item['aid'] = each[2]
                item['pts'] = each[3]
                item['mid'] = each[4]
                item['channel'] = channel
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
