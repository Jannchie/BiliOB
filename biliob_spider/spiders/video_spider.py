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


class VideoSpider(scrapy.spiders.Spider):
    name = "videoSpider"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.VideoPipeline': 300,
        }
    }

    def __init__(self):
        self.db = db  # 获得数据库的句柄
        self.coll = self.db['video']  # 获得collection的句柄

    def start_requests(self):
        # 只需要aid
        c = self.coll.find(
            {'$or': [{'focus': True}, {'forceFocus': True}]}, {'aid': 1})
        x = 0
        aid_list = []
        for each_doc in c:
            x = x + 1
            aid_list.append(each_doc['aid'])
        i = 0
        while aid_list != []:
            if i == 0:
                aid_str = ''
            aid_str += str(aid_list.pop()) + ','
            i = i + 1
            if i == 50 or aid_list == []:
                i = 0
                yield Request(
                    "https://api.bilibili.com/x/article/archives?ids=" +
                    aid_str.rstrip(','))

    def parse(self, response):
        try:
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
                    if (subChannel not in sub_channel_2_channel):
                        item['channel'] = '未知'
                    else:
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
                yield item

        except Exception as error:
            # 出现错误时打印错误日志
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
