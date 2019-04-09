# coding=utf-8
import json
import logging
import os
import re
from datetime import datetime

import jieba
import jieba.analyse
import scrapy
from pymongo import MongoClient
from scrapy.http import Request
from scrapy_redis.spiders import RedisSpider

from danmaku_spider.items import DanmakuAggregateItem

env_dist = os.environ


class DanmakuAggregateSpider(RedisSpider):

    name = "DanmakuAggregate"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'DOWNLOAD_DELAY': 1
    }
    CID_API = "https://api.bilibili.com/x/web-interface/view?aid={aid}"
    DANMAKU_API = "https://api.bilibili.com/x/v1/dm/list.so?oid={oid}"

    def __init__(self):
        jieba.load_userdict('../biliob_analyzer/dict.txt')
        self.client = MongoClient(env_dist['BILIOB_MONGO_SERVER'], 27017)
        self.client.admin.authenticate(env_dist['BILIOB_MONGO_USER'],
                                       env_dist['BILIOB_MONGO_PASSWD'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['video']  # 获得collection的句柄

    def q_to_b(self, q_str):
        """全角转半角"""
        b_str = ""
        for uchar in q_str:
            inside_code = ord(uchar)
            if inside_code == 12288:  # 全角空格直接转换
                inside_code = 32
            elif 65374 >= inside_code >= 65281:  # 全角字符（除空格）根据关系转化
                inside_code -= 65248
            b_str += chr(inside_code)
        return b_str

    def parse(self, response):
        try:
            j = json.loads(response.body)
            url_list = response.url.split('&')
            if len(url_list) == 2:
                object_id = url_list[1]
            else:
                object_id == None
            if j['code'] == -403:
                aid = response.url[50:]
                print('https://api.bilibili.com/x/article/archives?ids={}'.format(aid))
                yield Request('https://api.bilibili.com/x/article/archives?ids={}'.format(aid),
                              callback=self.getCidPlanB, meta={'aid': aid, 'object_id': object_id})
            else:
                aid = j['data']['aid']
                pages = j['data']['pages']
                for each_page in pages:
                    duration = each_page['duration']
                    p_name = each_page['part']
                    page_number = each_page['page']
                    cid = each_page['cid']
                    yield Request(self.DANMAKU_API.format(oid=cid), callback=self.parseDanmaku,
                                  meta={'duration': duration,
                                        'p_name': p_name,
                                        'page_number': page_number,
                                        'aid': aid, 'object_id': object_id})
        except Exception as error:
            # 出现错误时存入出错集合
            self.db['error'].insert_one(
                {'aid': int(aid), 'url': response.url, 'error': error})

    def getCidPlanB(self, response):
        try:
            aid = response.meta['aid']
            object_id = response.meta['object_id']
            cid = json.loads(response.body)['data'][aid]['cid']
            duration = json.loads(response.body)['data'][aid]['duration']
            yield Request(self.DANMAKU_API.format(oid=cid), callback=self.parseDanmaku, meta={'object_id': object_id, 'duration': duration, 'p_name': '', 'page_number': 1, 'aid': int(aid)})
        except Exception as error:
            # 出现错误时存入出错集合
            self.db['error'].insert_one(
                {'aid': int(aid), 'url': response.url, 'error': error})

    def parseDanmaku(self, response):
        try:
            duration = response.meta['duration']

            # 全角转半角，转大写
            danmaku_text = self.q_to_b(
                " ".join(response.xpath("d/text()").extract()).upper())

            # 自实现太low，使用自带关键字
            word_frequency = dict(jieba.analyse.extract_tags(danmaku_text, topK=50, withWeight=True, allowPOS=(
                'ns', 'n', 'vn', 'v', 'nr', 'un', 'x', 'j', 'i', 'l', 'nz', 'eng', 'o')))
            # 计算弹幕密度
            danmaku_attr = list(map(lambda x: x.split(
                ","), response.xpath("d/@p").extract()))
            tick = duration / 50
            danmaku_density = {}
            danmaku_density = [0 for i in range(50)]
            for each_attr in danmaku_attr:
                t = float(each_attr[0])
                if t > duration:
                    continue
                index = int(t // tick)
                danmaku_density[index] += 1
            item = DanmakuAggregateItem()
            item['object_id'] = response.meta['object_id']
            item['aid'] = response.meta['aid']
            item['duration'] = duration
            item['word_frequency'] = word_frequency
            item['p_name'] = response.meta['p_name']
            item['danmaku_density'] = danmaku_density
            item['page_number'] = response.meta['page_number']
            yield item
        except Exception as error:
            # 出现错误时存入出错集合
            self.db['error'].insert_one(
                {'aid': int(response.meta['aid']), 'url': response.url, 'error': error})
