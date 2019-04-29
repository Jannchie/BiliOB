
import schedule
import threading
from time import sleep

from biliob_tracer.task import ProgressTask
import requests
import redis
from lxml import etree
import json
import requests
from db import redis_connection
from db import db
import logging
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s @ %(name)s: %(message)s')
logger = logging.getLogger(__name__)

VIDEO_URL = "https://api.bilibili.com/x/article/archives?ids={aid}"
VIDEO_KEY = "videoRedis:start_urls"
AUTHOR_URL = "https://api.bilibili.com/x/web-interface/card?mid={mid}"
AUTHOR_KEY = "authorRedis:start_urls"
DANMAKU_FROM_AID_URL = "https://api.bilibili.com/x/web-interface/view?aid={aid}"
DANMAKU_KEY = "DanmakuAggregate:start_urls"
SITEINFO_URL = 'https://api.bilibili.com/x/web-interface/online'
SITEINFO_KEY = "site:start_urls"


def auto_crawl_bangumi():
    logger.info("生成番剧国创待爬链接")
    redis_connection.rpush("bangumiAndDonghua:start_urls",
                           "https://www.bilibili.com/ranking/bangumi/167/0/7")
    redis_connection.rpush("bangumiAndDonghua:start_urls",
                           "https://www.bilibili.com/ranking/bangumi/13/0/7")
    pass


def auto_add_video():
    logger.info("生成作者最新发布的视频的待爬链接")
    coll = db['author']
    c = coll.find(
        {'$or': [{'focus': True}, {'forceFocus': True}]}, {'mid': 1})
    for each_doc in c:
        URL = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid={}&pagesize=10&page=1&order=pubdate'.format(
            each_doc['mid'])
        redis_connection.rpush("videoAutoAdd:start_urls", URL)


def auto_add_author():
    logger.info("生成排行榜待爬链接")
    start_urls = [
        'https://www.bilibili.com/ranking',
        'https://www.bilibili.com/ranking/all/1/0/3',
        'https://www.bilibili.com/ranking/all/168/0/3',
        'https://www.bilibili.com/ranking/all/3/0/3',
        'https://www.bilibili.com/ranking/all/129/0/3',
        'https://www.bilibili.com/ranking/all/4/0/3',
        'https://www.bilibili.com/ranking/all/36/0/3',
        'https://www.bilibili.com/ranking/all/160/0/3',
        'https://www.bilibili.com/ranking/all/119/0/3',
        'https://www.bilibili.com/ranking/all/155/0/3',
        'https://www.bilibili.com/ranking/all/5/0/3',
        'https://www.bilibili.com/ranking/all/181/0/3'
    ]
    for each in start_urls:
        redis_connection.rpush('authorAutoAdd:start_urls', each)


def crawlOnlineTopListData():
    ONLINE_URL = 'https://www.bilibili.com/video/online.html'
    response = requests.get(ONLINE_URL)
    data_text = etree.HTML(response.content.decode(
        'utf8')).xpath('//script/text()')[-2]
    j = json.loads(data_text.lstrip('window.__INITIAL_STATE__=')[:-122])
    for each_video in j['onlineList']:
        aid = each_video['aid']
        mid = each_video['owner']['mid']
        if mid not in [7584632, 928123]:
            priorityAuthorCrawlRequest(mid)
        priorityVideoCrawlRequest(aid)


def update_author():
    logger.info("开始生成每日作者待爬链接")
    coll = db['author']
    filter_dict = {
        '$or': [{
            'focus': True
        }, {
            'forceFocus': True
        }]
    }
    cursor = coll.find(filter_dict, {"mid": 1}).batch_size(200)
    total = coll.count_documents(filter_dict)
    t = ProgressTask('作者每日自动爬取', total)

    for each_doc in cursor:
        t.current_value += 1
        redis_connection.rpush(
            AUTHOR_KEY, AUTHOR_URL.format(mid=each_doc['mid']))


def update_all_video():
    logger.info("开始生成每周全站视频待爬链接")
    coll = db['video']
    cursor = coll.find({}, {"aid": 1}).batch_size(200)
    total = coll.count_documents({'focus': True})
    send_aids(total, cursor)


def update_video():
    logger.info("开始生成每日视频待爬链接")
    coll = db['video']
    cursor = coll.find({
        'focus': True
    }, {"aid": 1}).batch_size(200)

    total = coll.count_documents({'focus': True})
    send_aids(total, cursor)


def send_aids(total, cursor):
    t = ProgressTask('视频每日自动爬取', total)
    logger.info("数量：{}".format(total))
    aid_list = ''
    i = 0
    for each_doc in cursor:
        aid_list += str(each_doc['aid']) + ','
        i += 1
        if i == 100:
            t.current_value += 100
            redis_connection.rpush(
                VIDEO_KEY, VIDEO_URL.format(aid=aid_list[:-1]))
            aid_list = ''
            i = 0


def auto_crawl_task():
    while True:
        schedule.run_pending()
        sleep(60)


def run_threaded(job_func):
    job_thread = threading.Thread(target=job_func)
    job_thread.start()


def sendAuthorCrawlRequest(mid):
    redis_connection.rpush(AUTHOR_KEY, AUTHOR_URL.format(mid=mid))


def sendVideoCrawlRequest(aid):
    redis_connection.rpush(VIDEO_KEY, VIDEO_URL.format(aid=aid))


def priorityAuthorCrawlRequest(mid):
    redis_connection.lpush(AUTHOR_KEY, AUTHOR_URL.format(mid=mid))


def priorityVideoCrawlRequest(aid):
    redis_connection.lpush(VIDEO_KEY, VIDEO_URL.format(aid=aid))


def sendSiteInfoCrawlRequest():
    redis_connection.rpush(SITEINFO_KEY, SITEINFO_URL)


def run():
    while True:
        schedule.run_pending()
        sleep(60)


schedule.every().day.at('01:00').do(run_threaded, update_author)
schedule.every().day.at('07:00').do(run_threaded, update_video)
schedule.every().day.at('14:00').do(run_threaded, auto_add_author)
schedule.every().day.at('16:50').do(run_threaded, auto_crawl_bangumi)
schedule.every().day.at('22:00').do(run_threaded, auto_add_video)
schedule.every().week.do(run_threaded, update_all_video)
schedule.every().hour.do(run_threaded, sendSiteInfoCrawlRequest)
schedule.every(1).minutes.do(run_threaded, crawlOnlineTopListData)
