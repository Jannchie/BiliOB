# coding=utf-8
import scrapy
from mail import mailer
from scrapy.http import Request
from biliob_spider.items import VideoAndAuthorItem
import time
import json
import logging
from pymongo import MongoClient
import datetime
from util import sub_channel_2_channel


class StrongSpider(scrapy.spiders.Spider):
    name = "strong"
    allowed_domains = ["bilibili.com"]
    start_urls = ['https://www.bilibili.com/video/online.html']
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.StrongPipeline': 300
        },
        'DOWNLOAD_DELAY': 10
    }

    def parse(self, response):
        try:
            video_list = response.xpath('//*[@id="app"]/div[2]/div[2]/div')
            # 为了爬取分区、粉丝数等数据，需要进入每一个视频的详情页面进行抓取
            href_list = video_list.xpath('./a/@href').extract()
            for i in range(len(href_list)):
                # 为了爬取分区等数据，需要进入每一个视频的详情页面进行抓取
                yield Request(
                    "https://api.bilibili.com/x/article/archives?ids=" +
                    href_list[i][9:-1],
                    callback=self.detailParse)
        except Exception as error:
            # 出现错误时打印错误日志
            mailer.send(
                to=["604264970@qq.com"],
                subject="BiliobSpiderError",
                body="{}\n{}".format(response.url, error),
            )
            logging.error("视频爬虫在解析时发生错误")
            logging.error(response.url)
            logging.error(error)

    def detailParse(self, response):
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
                current_date = datetime.datetime.now()
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
                item = VideoAndAuthorItem()
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
                item['data_video'] = data
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
                yield Request(
                    "https://api.bilibili.com/x/web-interface/card?mid=" +
                    str(mid), meta={'item': item},
                    method='GET', callback=self.authorParse)

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

    def authorParse(self, response):
        try:
            item = response.meta['item']
            j = json.loads(response.body)
            name = j['data']['card']['name']
            mid = j['data']['card']['mid']
            sex = j['data']['card']['sex']
            face = j['data']['card']['face']
            fans = j['data']['card']['fans']
            attention = j['data']['card']['attention']
            level = j['data']['card']['level_info']['current_level']
            official = j['data']['card']['Official']['title']
            archive = j['data']['archive_count']
            article = j['data']['article_count']
            face = j['data']['card']['face']
            item['mid'] = int(mid)
            item['name'] = name
            item['face'] = face
            item['official'] = official
            item['sex'] = sex
            item['level'] = int(level)
            item['data_author'] = {
                'fans': int(fans),
                'attention': int(attention),
                'archive': int(archive),
                'article': int(article),
                'datetime': datetime.datetime.now()
            }
            item['c_fans'] = int(fans)
            item['c_attention'] = int(attention)
            item['c_archive'] = int(archive)
            item['c_article'] = int(article)
            yield Request(
                "https://api.bilibili.com/x/space/upstat?mid={mid}".format(
                    mid=str(mid)),
                meta={'item': item},
                method='GET',
                callback=self.parse_view)
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

    def parse_view(self, response):
        j = json.loads(response.body)
        archive_view = j['data']['archive']['view']
        article_view = j['data']['article']['view']
        item = response.meta['item']
        item['data_author']['archiveView'] = archive_view
        item['data_author']['articleView'] = article_view
        item['c_archive_view'] = int(archive_view)
        item['c_article_view'] = int(article_view)
        yield item
