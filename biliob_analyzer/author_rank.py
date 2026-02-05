from db import db
from db import db
import datetime
import logging
from pymongo import DESCENDING
from biliob_tracer.task import ProgressTask
coll = db['author']  # 获得collection的句柄

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s @ %(name)s: %(message)s')
logger = logging.getLogger(__name__)


def format_p_rank(i, count):
    return round(i/count * 100, 2)


def calculate_author_rank():
    task_name = "计算作者排名数据"
    keys = ['cFans', 'cArchive_view', 'cArticle_view']
    count = coll.count_documents({keys[0]: {'$exists': 1}})
    t = ProgressTask(task_name, count * len(keys), collection=db['tracer'])
    for each_key in keys:
        logger.info("开始计算作者{}排名".format(each_key))
        i = 1
        authors = coll.find({each_key: {'$exists': 1}}, {'mid': 1, 'rank': 1, each_key: 1}).batch_size(
            300).sort(each_key, DESCENDING)
        if each_key == 'cFans':
            each_rank = 'fansRank'
            each_d_rank = 'dFansRank'
            each_p_rank = 'pFansRank'
        elif each_key == 'cArchive_view':
            each_rank = 'archiveViewRank'
            each_d_rank = 'dArchiveViewRank'
            each_p_rank = 'pArchiveViewRank'
        elif each_key == 'cArticle_view':
            each_rank = 'articleViewRank'
            each_d_rank = 'dArticleViewRank'
            each_p_rank = 'pArticleViewRank'
        for each_author in authors:
            t.current_value += 1
            logger.info("计算{}排名".format(each_author['mid']))
            # 如果没有data 直接下一个
            if each_key in each_author:
                # 如果已经计算过rank
                if 'rank' in each_author:
                    rank = each_author['rank']
                    if each_rank in each_author['rank']:
                        rank[each_d_rank] = each_author['rank'][each_rank] - i
                    else:
                        rank[each_d_rank] = 0
                    rank[each_rank] = i
                    rank[each_p_rank] = format_p_rank(i, count)
                else:
                    # 初始化
                    rank = {
                        each_rank: i,
                        each_d_rank: 0,
                        each_p_rank: format_p_rank(i, count)
                    }
            if each_author[each_key] == 0:
                if 'rank' in each_author:
                    rank = each_author['rank']
                    rank[each_d_rank] = 0
                    rank[each_rank] = -1
                    rank[each_p_rank] = -1
                else:
                    rank = {
                        each_rank: -1,
                        each_d_rank: 0,
                        each_p_rank: -1
                    }
            if each_key == 'cArticle_view':
                rank['updateTime'] = datetime.datetime.now()
            coll.update_one({'mid': each_author['mid']}, {
                '$set': {
                    'rank': rank,
                }
            })
            i += 1
    t.current_value = t.total_value
    logger.info("计算作者排名结束")
