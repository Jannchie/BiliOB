# coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import VideoItem
from datetime import datetime
import time
import json
import logging
from pymongo import MongoClient
from db import db
from util import sub_channel_2_channel
from scrapy_redis.spiders import RedisSpider
from db import redis_connect_string
from biliob_tracer.task import SpiderTask


class VideoSpiderWithRedis(RedisSpider):
    name = "videoRedis"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.VideoPipeline': 300,
        }
    }

    def __init__(self):
        self.db = db
        self.coll = self.db['video']  # 获得collection的句柄
        self.task = SpiderTask('视频数据更新爬虫', collection=self.db['tracer'])

    def parse(self, response):
        try:
            self.task.crawl_count += 1
            r = json.loads(response.body)
            d = r["data"]
            keys = list(d.keys())
            for each_key in keys:

                aid = d[each_key]['stat']['aid']
                author = d[each_key]['owner']['name']
                mid = d[each_key]['owner']['mid']
                view = d[each_key]['stat']['view']
                favorite = d[each_key]['stat']['favorite']
                danmaku = d[each_key]['stat']['danmaku']
                coin = d[each_key]['stat']['coin']
                share = d[each_key]['stat']['share']
                like = d[each_key]['stat']['like']
                current_date = datetime.now()
                data = {
                    'view': view,
                    'favorite': favorite,
                    'danmaku': danmaku,
                    'coin': coin,
                    'share': share,
                    'like': like,
                    'datetime': current_date
                }

                subChannel = d[each_key]['tname']
                title = d[each_key]['title']
                date = d[each_key]['pubdate']
                tid = d[each_key]['tid']
                pic = d[each_key]['pic']
                item = VideoItem()
                item['current_view'] = view
                item['current_favorite'] = favorite
                item['current_danmaku'] = danmaku
                item['current_coin'] = coin
                item['current_share'] = share
                item['current_like'] = like
                item['current_datetime'] = current_date
                item['aid'] = aid
                item['mid'] = mid
                item['pic'] = pic
                item['author'] = author
                item['data'] = data
                item['title'] = title
                item['subChannel'] = subChannel
                item['datetime'] = date

                if subChannel != '':
                    item['channel'] = sub_channel_2_channel[subChannel]
                elif subChannel == '资讯':
                    if tid == 51:
                        item['channel'] == '番剧'
                    if tid == 170:
                        item['channel'] == '国创'
                    if tid == 159:
                        item['channel'] == '娱乐'
                else:
                    item['channel'] = None

                url_list = response.url.split('&')
                if len(url_list) == 2:
                    item['object_id'] = url_list[1]
                else:
                    item['object_id'] = None
                yield item

        except Exception as error:
            # 出现错误时打印错误日志
            self.task.crawl_failed += 1
            if r['code'] == -404:
                return
            mailer.send(
                to=["604264970@qq.com"],
                subject="BiliobSpiderError",
                body="{}\n{}\n{}".format(item, response.url, error),
            )
            logging.error("视频爬虫在解析时发生错误")
            logging.error(item)
            logging.error(response.url)
            logging.error(error)
