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
from biliob_spider.spiders.video_spider import VideoSpider


class VideoSpiderAll(VideoSpider):
    name = "videoSpiderAll"
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
        c = self.coll.find({}, {'aid': 1})

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
