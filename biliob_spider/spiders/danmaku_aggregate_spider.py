# coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import DanmakuAggregateItem
from datetime import datetime
import time
import json
import logging
from pymongo import MongoClient
from db import settings
from util import sub_channel_2_channel
from scrapy_redis.spiders import RedisSpider
from db import redis_connect_string
import jieba
import jieba.analyse
import re

jieba.load_userdict('./biliob_analyzer/dict.txt')


def q_to_b(q_str):
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


class DanmakuAggregateSpider(RedisSpider):
    name = "DanmakuAggregate"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.DanmakuAggregatePipeline': 300,
        },
        'DOWNLOAD_DELAY': 1
    }
    CID_API = "https://api.bilibili.com/x/web-interface/view?aid={aid}"
    DANMAKU_API = "https://api.bilibili.com/x/v1/dm/list.so?oid={oid}"
    PATTERN = r"[0-9a-zA-Z\u4e00-\u9fa5\u30a1-\u30f6\u3041-\u3093\uFF00-\uFFFF\u4e00-\u9fa5]+"

    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['video']  # 获得collection的句柄

    def parse(self, response):
        try:
            j = json.loads(response.body)
            if j['code'] == -403:
                aid = response.url[50:]
                print('https://api.bilibili.com/x/article/archives?ids={}'.format(aid))
                yield Request('https://api.bilibili.com/x/article/archives?ids={}'.format(aid),
                              callback=self.getCidPlanB, meta={'aid': aid})
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
                                        'aid': aid})
        except Exception as error:
            # 出现错误时打印错误日志
            if response['code'] == -404:
                return
            mailer.send(
                to=["604264970@qq.com"],
                subject="BiliobSpiderError",
                body="{}\n{}".format(response.url, error),
            )
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)

    def getCidPlanB(self, response):
        aid = response.meta['aid']
        cid = json.loads(response.body)['data'][aid]['cid']
        duration = json.loads(response.body)['data'][aid]['duration']
        yield Request(self.DANMAKU_API.format(oid=cid), callback=self.parseDanmaku, meta={'duration': duration, 'p_name': '', 'page_number': 1, 'aid': int(aid)})

    def parseDanmaku(self, response):
        duration = response.meta['duration']
        danmaku_text = q_to_b(
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

        item['aid'] = response.meta['aid']
        item['duration'] = duration
        item['word_frequency'] = word_frequency
        item['p_name'] = response.meta['p_name']
        item['danmaku_density'] = danmaku_density
        item['page_number'] = response.meta['page_number']
        yield item
