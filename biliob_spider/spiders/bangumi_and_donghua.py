# coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import BangumiOrDonghuaItem
import time
import datetime
import json
from scrapy_redis.spiders import RedisSpider
from biliob_tracer.task import ExistsTask
from db import db


class BangumiAndDonghuaSpider(RedisSpider):
    name = "bangumiAndDonghua"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.BangumiAndDonghuaPipeLine': 200
        }
    }

    def __init__(self):
        ExistsTask("番剧动画爬虫", collection=db['tracer'])

    def parse(self, response):
        try:
            j = json.loads(response.xpath(
                "//script[3]/text()").extract()[0][len('window.__INITIAL_STATE__='):].split(';')[0])
            for each in j['rankList']:
                item = BangumiOrDonghuaItem()
                item['title'] = each['title']
                item['cover'] = each['cover']
                # item['square_cover'] = each['square_cover']
                # item['is_finish'] = each['is_finish']
                # item['is_started'] = each['is_started']
                item['newest_ep_index'] = each['new_ep']['index_show']
                item['data'] = {
                    'danmaku': each['stat']['danmaku'],
                    'watch': each['stat']['follow'],
                    'play': each['stat']['view'],
                    'pts': each['pts'],
                    'review': each['video_review'],
                    'datetime': datetime.datetime.now()
                }

                if response.url == 'https://www.bilibili.com/ranking/bangumi/13/0/7':
                    item['collection'] = 'bangumi'
                elif response.url == 'https://www.bilibili.com/ranking/bangumi/167/0/7':
                    item['collection'] = 'donghua'
                yield item
        except Exception as error:
            # 出现错误时打印错误日志
            mailer.send(
                to=["604264970@qq.com"],
                subject="BiliobSpiderError",
                body="{}\n{}".format(response.url, error),
            )
