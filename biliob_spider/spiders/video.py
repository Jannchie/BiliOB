#coding=utf-8
import scrapy
from scrapy.http import Request
from biliob_spider.items import VideoItem
import time
import json
import logging
from dateutil import parser
from pymongo import MongoClient
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
    '数码': '科技',
    '日常': '生活',
    '明星': '娱乐',
    '星海': '科技',
    '服饰': '时尚',
    '机械': '科技',
    '桌游棋牌': '游戏',
    '欧美电影': '电影',
    '汽车': '科技',
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
    '生活': '生活',
    '游戏': '游戏'
}
class VideoSpider(scrapy.spiders.Spider):
    name = "videoSpider"
    allowed_domains = ["bilibili.com"]
    start_urls = []
    custom_settings = {
        'ITEM_PIPELINES': {
            'biliob_spider.pipelines.VideoPipeline': 300,
        },
        'DOWNLOAD_DELAY' : 1
    }
    def __init__(self,start_aid=1,length=99999999,limit_view=50000, *args, **kwargs):
        # super(HighSpeedVideoSpider2, self).__init__(*args, **kwargs)
        print("开始的av号为:" + str(start_aid) + ",计划抓取的视频个数为：" + str(length))
        self.start_aid = int(start_aid)
        self.length = int(length)
        self.limit_view = limit_view
    def start_requests(self):
        i = (x for x in range(self.start_aid, self.start_aid + self.length))
        while True:
            aid_str = ''
            for j in range(100):
                aid_str += str(next(i))+','
            yield Request("https://api.bilibili.com/x/article/archives?ids=" + aid_str.rstrip(','))
    def parse(self, response):
        try:
            r = json.loads(response.body)
            d = r["data"]
            keys = list(d.keys())
            for each_key in keys:
                aid = d[each_key]['stat']['aid']
                author = d[each_key]['owner']['name']
                view = d[each_key]['stat']['view']
                favorite = d[each_key]['stat']['favorite']
                danmaku = favorite = d[each_key]['stat']['danmaku']
                coin = d[each_key]['stat']['coin']
                share = d[each_key]['stat']['share']
                like = d[each_key]['stat']['like']
                dislike = d[each_key]['stat']['dislike']
                subChannel = d[each_key]['tname']
                title = d[each_key]['title']
                datetime = d[each_key]['pubdate']
                tid = d[each_key]['tid']
                item = VideoItem()
                item['aid'] = aid
                item['author'] = author
                item['view'] = view
                item['favorite'] = favorite
                item['coin'] = coin
                item['share'] = share
                item['like'] = like
                item['dislike'] = dislike
                item['danmaku'] = danmaku
                item['title'] = title
                item['subChannel'] = subChannel
                item['datetime'] = datetime
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
                
                # 只收录大于limit_view的视频
                if view > self.limit_view:
                    yield item
        except Exception as error:
            # 出现错误时打印错误日志
            if r['code'] == -404:
                return
            logging.error("视频爬虫在解析时发生错误")
            logging.error(item)
            logging.error(response.url)
            logging.error(error)
# scrapy crawl VideoTagSpider -a start_aid=26053983 -a length=2000000 -s JOBDIR=tag-07-21 -L INFO
