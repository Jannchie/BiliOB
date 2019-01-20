#coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import BangumiItem
import time
import datetime
import json

class DonghuaSpider(scrapy.spiders.Spider):
    name = "donghua"
    allowed_domains = ["bilibili.com"]
    start_urls = ["https://www.bilibili.com/ranking/bangumi/167/0/7"]
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.DonghuaPipeLine': 200
        }
    }

    def parse(self, response):
        try:
            j = json.loads(response.xpath("//script[3]/text()").extract()[0][len('window.__INITIAL_STATE__='):].split(';')[0])
            for each in j['rankList']:
                item = BangumiItem()
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
                yield item
        except Exception as error:
            # 出现错误时打印错误日志
            mailer.send(
                to=["604264970@qq.com"],
                subject="BiliobSpiderError",
                body="{}\n{}".format(response.url, error),
            )


