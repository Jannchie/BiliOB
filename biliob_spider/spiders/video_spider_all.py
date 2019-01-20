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
from db import settings

sub_channel_2_channel = {
    'ASMR': '生活',
    'GMV': '游戏',
    'Korea相关': '娱乐',
    'MAD·AMV': '动画',
    'MMD·3D': '动画',
    'Mugen': '游戏',
    'OP/ED/OST': '音乐',
    'VOCALOID·UTAU': '音乐',
    '三次元舞蹈': '舞蹈',
    '三次元音乐': '音乐',
    '人力VOCALOID': '鬼畜',
    '人文·历史': '纪录片',
    '健身': '时尚',
    '其他': '生活',
    '其他国家': '电影',
    '军事': '纪录片',
    '动物圈': '生活',
    '华语电影': '电影',
    '单机游戏': '游戏',
    '原创音乐': '音乐',
    '国产剧': '电视剧',
    '国产动画': '国创',
    '国产原创相关': '国创',
    '宅舞': '舞蹈',
    '完结动画': '番剧',
    '官方延伸': '番剧',
    '布袋戏': '国创',
    '广告': '广告',
    '影视剪辑': '影视',
    '影视杂谈': '影视',
    '手工': '生活',
    '手机游戏': '游戏',
    '搞笑': '生活',
    '教程演示': '鬼畜',
    '数码': '数码',
    '日常': '生活',
    '明星': '娱乐',
    '星海': '科技',
    '服饰': '时尚',
    '机械': '科技',
    '桌游棋牌': '游戏',
    '欧美电影': '电影',
    '汽车': '科技',
    '游戏': '游戏',
    '海外剧': '电视剧',
    '演奏': '音乐',
    '演讲·公开课': '科技',
    '特摄': '影视',
    '电子竞技': '游戏',
    '短片': '影视',
    '短片·手书·配音': '动画',
    '社会·美食·旅行': '纪录片',
    '科学·探索·自然': '纪录片',
    '绘画': '生活',
    '综合': '动画',
    '综艺': '娱乐',
    '网络游戏': '游戏',
    '美妆': '时尚',
    '美食圈': '生活',
    '翻唱': '音乐',
    '舞蹈教程': '舞蹈',
    '资讯': '国创',
    '趣味科普人文': '科技',
    '运动': '生活',
    '连载动画': '番剧',
    '野生技术协会': '科技',
    '音MAD': '鬼畜',
    '音乐选集': '音乐',
    '音游': '游戏',
    '预告 资讯': '影视',
    '预告·资讯': '影视',
    '单机联机': '游戏',
    '鬼畜调教': '鬼畜',
    '演讲• 公开课': '科技',
    '国产电影': '电影',
    '日本电影': '电影',
    '番剧': '番剧',
    '国创': '国创',
    '鬼畜': '鬼畜',
    '电视剧': '电视剧',
    '动画': '动画',
    '时尚': '时尚',
    '娱乐': '娱乐',
    '电影': '电影',
    '舞蹈': '舞蹈',
    '科技': '科技',
    '生活': '生活',
    '音乐': '音乐',
    '纪录片': '纪录片',
    '手机平板': '数码',
    '电脑装机': '数码',
    '影音智能': '数码',
    '摄影摄像': '数码',
    '风尚标': '时尚',
    '电音': '音乐',
    '音乐综合': '音乐',
    'MV': '音乐',
    '音乐现场': '音乐',
    'T台': '时尚',
}


class VideoSpider(scrapy.spiders.Spider):
    name = "videoSpiderAll"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.VideoPipeline': 300,
        }
    }

    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
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
            if i == 100 or aid_list == []:
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
