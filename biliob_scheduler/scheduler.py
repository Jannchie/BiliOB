
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

from biliob_analyzer.author_rate_caculate import author_fans_rate_caculate

from biliob_analyzer.video_rank import compute_video_rank_table
from biliob_analyzer.author_rank import calculate_author_rank
from biliob_tracer.task import ExistsTask

from biliob_analyzer.video_rank import calculate_video_rank
from biliob_analyzer.author_fans_watcher import FansWatcher
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
    task_name = "生成番剧国创待爬链接"
    logger.info(task_name)
    t = ProgressTask(task_name, 1, collection=db['tracer'])
    redis_connection.rpush("bangumiAndDonghua:start_urls",
                           "https://www.bilibili.com/ranking/bangumi/167/0/7")
    redis_connection.rpush("bangumiAndDonghua:start_urls",
                           "https://www.bilibili.com/ranking/bangumi/13/0/7")
    t.current_value += 1


def auto_add_video():
    task_name = "生成作者最新发布的视频的待爬链接"
    logger.info(task_name)
    coll = db['author']
    doc_filter = {'$or': [{'focus': True}, {'forceFocus': True}]}
    total = coll.count_documents(doc_filter)
    c = coll.find(doc_filter, {'mid': 1})
    if total != 0:
        t = ProgressTask(task_name, total, collection=db['tracer'])
        for each_doc in c:
            t.current_value += 1
            URL = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid={}&pagesize=10&page=1&order=pubdate'.format(
                each_doc['mid'])
            redis_connection.rpush("videoAutoAdd:start_urls", URL)


def auto_add_author():
    task_name = "生成排行榜待爬链接"
    logger.info(task_name)
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
    t = ProgressTask(task_name, len(start_urls), collection=db['tracer'])
    for each in start_urls:
        t.current_value += 1
        redis_connection.rpush('authorAutoAdd:start_urls', each)


def crawlOnlineTopListData():
    task_name = "生成强力追踪待爬链接"
    logger.info(task_name)
    ONLINE_URL = 'https://www.bilibili.com/video/online.html'
    response = requests.get(ONLINE_URL)
    data_text = etree.HTML(response.content.decode(
        'utf8')).xpath('//script/text()')[-2]
    j = json.loads(data_text.lstrip('window.__INITIAL_STATE__=')[:-122])
    total = len(j['onlineList'])
    t = ProgressTask(task_name, total, collection=db['tracer'])
    for each_video in j['onlineList']:
        aid = each_video['aid']
        mid = each_video['owner']['mid']
        if mid not in [7584632, 928123]:
            priorityAuthorCrawlRequest(mid)
        priorityVideoCrawlRequest(aid)
        t.current_value += 1


def update_author():
    task_name = "生成每日作者待爬链接"
    logger.info(task_name)
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
    if total != 0:
        t = ProgressTask(task_name, total, collection=db['tracer'])
        for each_doc in cursor:
            t.current_value += 1
            redis_connection.rpush(
                AUTHOR_KEY, AUTHOR_URL.format(mid=each_doc['mid']))


def update_unfocus_video():
    task_name = "生成保守观测视频待爬链接"
    logger.info(task_name)
    doc_filter = {'focus': False}
    gen_video_link_by_filter(task_name, doc_filter)


def update_video():
    task_name = "生成每日视频待爬链接"
    logger.info(task_name)
    doc_filter = {'focus': True}
    gen_video_link_by_filter(task_name, doc_filter)


def gen_video_link_by_filter(task_name, doc_filter):
    coll = db['video']
    total = coll.count_documents(doc_filter)
    cursor = coll.find(doc_filter, {"aid": 1}).batch_size(200)
    send_aids(task_name, total, cursor)


def send_aids(task_name, total, cursor):
    if total == 0:
        return
    t = ProgressTask(task_name, total, collection=db['tracer'])
    aid_list = ''
    i = 0
    for each_doc in cursor:
        aid_list += str(each_doc['aid']) + ','
        i += 1
        logger.info(each_doc['aid'])
        if i == 100:
            t.current_value += i
            redis_connection.rpush(
                VIDEO_KEY, VIDEO_URL.format(aid=aid_list[:-1]))
            aid_list = ''
            i = 0
    t.current_value += i
    redis_connection.rpush(
        VIDEO_KEY, VIDEO_URL.format(aid=aid_list[:-1]))


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


def auto_crawl_task():
    task_name = "自动爬虫计划调度服务"
    logger.info(task_name)
    ExistsTask(task_name, update_frequency=60, collection=db['tracer'])
    while True:
        schedule.run_pending()
        sleep(60)


def gen_online():
    task_name = "生成在线人数爬取链接"
    t = ProgressTask(task_name, 1, collection=db['tracer'])
    ONLINE_URL = 'https://www.bilibili.com/video/online.html'
    redis_connection.rpush("online:start_urls", ONLINE_URL)
    t.current_value = 1


schedule.every().day.at('01:00').do(run_threaded, update_author)
schedule.every().day.at('07:00').do(run_threaded, update_video)
schedule.every().day.at('12:00').do(run_threaded, FansWatcher().watchBigAuthor)
schedule.every().day.at('13:00').do(run_threaded, author_fans_rate_caculate)
schedule.every().day.at('14:00').do(run_threaded, auto_add_author)
schedule.every().day.at('16:50').do(run_threaded, auto_crawl_bangumi)
schedule.every().day.at('22:00').do(run_threaded, auto_add_video)

schedule.every().wednesday.at('03:20').do(
    run_threaded, compute_video_rank_table)
schedule.every().monday.at('03:20').do(run_threaded, calculate_author_rank)

schedule.every().week.do(run_threaded, update_unfocus_video)
schedule.every().hour.do(run_threaded, sendSiteInfoCrawlRequest)
schedule.every(1).minutes.do(run_threaded, crawlOnlineTopListData)
schedule.every(15).minutes.do(run_threaded, gen_online)
