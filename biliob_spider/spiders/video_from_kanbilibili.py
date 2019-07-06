# coding=utf-8
import scrapy
from scrapy.http import Request
from biliob_spider.items import VideoItem
import datetime
import time
import json
import logging
from pymongo import MongoClient
from db import settings
from mail import mailer
from util import sub_channel_2_channel


class FromKan(scrapy.spiders.Spider):
    name = "fromkan"
    allowed_domains = ["kanbilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.VideoPipelineFromKan': 300,
        },
        'DOWNLOAD_DELAY': 0.5
    }

    def dateRange(self, beginDate, endDate):
        dates = []
        dt = datetime.datetime.strptime(beginDate, "%Y%m%d")
        date = beginDate[:]
        while date <= endDate:
            dates.append(date)
            dt = dt + datetime.timedelta(1)
            date = dt.strftime("%Y%m%d")
        return dates

    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['video']  # 获得collection的句柄

    def start_requests(self):
<<<<<<< HEAD
        dates = self.dateRange('20181001', '20190120')
        for each in dates:
            yield Request(
                'https://www.kanbilibili.com/json/all/{}/0_play_0.json'.format(
                    each),
                meta={'date': each})
=======
        dates = self.dateRange('20160414', '20190120')
        channel_list = [1, 13, 167, 3, 129, 4, 36,
                        153, 160, 119, 155, 165, 5, 11, 23]
        for each in dates:
            for each_channel in channel_list:
                yield Request(
                    'https://www.kanbilibili.com/json/all/{}/{}_play_0.json'.format(
                        each, each_channel), meta={'date': each})
>>>>>>> ccbc48ffa2e158f353a8174aa02f6160e68a8575

    def parse(self, response):
        try:
            if response.status == 404:
                return
            r = json.loads(response.body)
            for each in r:
                aid = each['aid']
                author = each['name']
                mid = each['mid']
                view = each['playTotal']
                favorite = each['favoritesTotal']
                danmaku = each['danmakuTotal']
                coin = None
                share = None
                like = None
                date = response.meta['date']
                date_str = '{}-{}-{}'.format(date[:4], date[4:6], date[6:8])
                current_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")

                data = {
                    'view': view,
                    'favorite': favorite,
                    'danmaku': danmaku,
                    'coin': coin,
                    'share': share,
                    'like': like,
                    'datetime': current_date
                }

                subChannel = None
                tid = None
                title = each['title']
                date = each['created']
                pic = 'http:' + each['pic']
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
                if author == '腾讯动漫' or author == '哔哩哔哩番剧':
                    continue
                self.coll.find_one({'aid': aid})
                d = self.coll.find_one({'aid': aid})
                flag = 0
                if d != None and 'data' in d:
                    if 'subChannel' in d:
                        item['subChannel'] = d['subChannel']
                    if 'channel' in d:
                        item['channel'] = d['channel']
                    for each_data in d['data']:
                        data_date = each_data['datetime'].strftime("%Y-%m-%d")
                        if data_date == date_str:
                            flag = 1
                            break
                if flag == 0:
                    yield item

        except Exception as error:
            # 出现错误时打印错误日志

            mailer.send(
                to=["604264970@qq.com"],
                subject="BiliobSpiderError",
                body="{}\n{}\n{}".format(item, response.url, error),
            )
            logging.error("视频爬虫在解析时发生错误")
            logging.error(item)
            logging.error(response.url)
            logging.error(error)
