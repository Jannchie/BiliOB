from db import db
from db import db
import datetime
import logging
from pymongo import DESCENDING
from time import sleep

from biliob_tracer.task import ProgressTask


def format_p_rank(i, count):
    return round(i / count * 100, 2)


def compute_video_rank_table():
    task_name = '计算视频排名对照表'
    coll = db['video']  # 获得collection的句柄
    count = coll.estimated_document_count()
    top_n = 60
    print(count)
    keys = ['cView', 'cLike', 'cDanmaku', 'cFavorite', 'cCoin', 'cShare']
    task = ProgressTask(task_name, top_n * len(keys), collection=db['tracer'])
    o = {}
    skip = int(count / 100)
    for each_key_index in range(len(keys)):
        each_key = keys[each_key_index]
        o[each_key] = {}
        o['name'] = 'video_rank'
        o[each_key]['rate'] = []
        i = 1
        last_value = 9999999999
        # logger.info("开始计算视频{}排名对照表".format(each_key))
        video = coll.find({}, {
            'title': 1}).limit(200).sort(each_key, DESCENDING).batch_size(200)
        top = 1
        for each_video in list(video):
            o[each_key][each_video['title']] = top
            top += 1

        while i <= top_n:
            task.current_value = i + top_n * each_key_index
            video = list(coll.find({each_key: {'$lt': last_value}}, {
                each_key: 1}).limit(1).skip(skip).sort(each_key, DESCENDING))
            print(video)
            if len(video) != 0:
                video = video[0]
            else:
                i += 1
                continue
            if each_key not in video:
                break
            last_value = video[each_key]
            o[each_key]['rate'].append(last_value)
            print(last_value)
            i += 1
    o['update_time'] = datetime.datetime.now()
    output_coll = db['rank_table']
    output_coll.update_one({'name': 'video_rank'}, {'$set': o}, upsert=True)


def calculate_video_rank(sleep_time=0.03):
    coll = db['video']  # 获得collection的句柄

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s] %(levelname)s @ %(name)s: %(message)s')
    logger = logging.getLogger(__name__)

    logger.info("开始计算视频数据排名")

    i = 1

    keys = ['cView', 'cLike', 'cDanmaku', 'cFavorite', 'cCoin', 'cShare']
    for each_key in keys:
        logger.info("开始计算视频{}排名".format(each_key))
        i = 1
        videos = coll.find({each_key: {'$exists': 1}}, {'aid': 1, 'rank': 1, each_key: 1}).batch_size(
            200).sort(each_key, DESCENDING)
        each_rank = each_key + 'Rank'
        each_d_rank = 'd' + each_key[1:] + 'Rank'
        each_p_rank = 'p' + each_key[1:] + 'Rank'
        count = coll.count_documents({})

        for each_video in videos:
            logger.info("[aid]{}".format(each_video['aid']))
            sleep(sleep_time)
            # 如果没有data 直接下一个
            if each_key in each_video:
                if 'rank' in each_video:
                    rank = each_video['rank']
                    if each_rank in each_video['rank']:
                        rank[each_d_rank] = each_video['rank'][each_rank] - i
                    else:
                        rank[each_d_rank] = -1
                    rank[each_rank] = i
                    rank[each_p_rank] = format_p_rank(i, count)
                else:
                    rank = {
                        each_rank: i,
                        each_d_rank: 0,
                        each_p_rank: format_p_rank(i, count)
                    }
            if each_video[each_key] == 0:
                if 'rank' in each_video:
                    rank = each_video['rank']
                    rank[each_d_rank] = 0
                    rank[each_rank] = -1
                    rank[each_p_rank] = -1
                else:
                    rank = {
                        each_rank: -1,
                        each_d_rank: 0,
                        each_p_rank: -1
                    }
            if each_key == keys[-1]:
                rank['updateTime'] = datetime.datetime.now()
            coll.update_one({'aid': each_video['aid']}, {
                '$set': {
                    'rank': rank,
                }
            })
            i += 1

        logger.info("完成计算视频数据排名")
