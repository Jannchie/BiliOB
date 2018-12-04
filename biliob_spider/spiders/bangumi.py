#coding=utf-8
import scrapy
from scrapy.http import Request
from biliob_spider.items import BangumiItem
import time
import datetime
import json


class BangumiSpider(scrapy.spiders.Spider):
    name = "bangumi"
    allowed_domains = ["bilibili.com"]
    start_urls = ["https://www.bilibili.com/ranking/bangumi/13/0/7"]
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.BangumiPipeLine': 200
        }
    }

    def parse(self, response):
        j = json.loads(response.xpath("//script[3]/text()").extract()[0][len('window.__INITIAL_STATE__='):].split(';')[0])
        for each in j['rankList']:
            item = BangumiItem()
            item['title'] = each['title']
            item['cover'] = each['cover']
            item['square_cover'] = each['square_cover']
            item['is_finish'] = each['is_finish']
            item['is_started'] = each['is_started']
            item['newest_ep_index'] = each['newest_ep_index']
            item['data'] = {
                'danmaku': each['dm_count'],
                'watch': each['fav'],
                'play': each['play'],
                'pts': each['pts'],
                'review': each['video_review'],
                'datetime': datetime.datetime.now()
            }
            yield item