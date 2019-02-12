from db import settings
from db import db
import datetime
import logging
from pymongo import DESCENDING
coll = db['author']  # 获得collection的句柄

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s @ %(name)s: %(message)s')
logger = logging.getLogger(__name__)

logger.info("开始计算作者粉丝排名")
i = 1
authors = coll.find({}, {'mid': 1, 'rank': 1, 'cFans': 1}).batch_size(
    20).sort('cFans', DESCENDING)
for each_author in authors:
    # 如果没有data 直接下一个
    if 'cFans' in each_author:
        if 'rank' in each_author:
            rank = each_author['rank']
            rank['fansRank'] = i
        else:
            rank = {
                'fansRank': i
            }
        i += 1
        coll.update_one({'mid': each_author['mid']}, {
            '$set': {
                'rank': rank,
            }
        })
    pass

logger.info("开始计算作者播放排名")
authors = coll.find({}, {'mid': 1, 'rank': 1, 'cArchive_view': 1}).batch_size(
    20).sort('cArchive_view', DESCENDING)
for each_author in authors:
    # 如果没有data 直接下一个
    if 'cArchive_view' in each_author:
        if 'rank' in each_author:
            rank = each_author['rank']
            rank['archiveViewRank'] = i
        else:
            rank = {
                'archiveViewRank': i
            }
        i += 1
        coll.update_one({'mid': each_author['mid']}, {
            '$set': {
                'rank': rank,
            }
        })
    pass

logger.info("开始计算作者专栏排名")
i = 1
authors = coll.find({}, {'mid': 1, 'rank': 1, 'cArticle_view': 1}).batch_size(
    20).sort('cArticle_view', DESCENDING)
for each_author in authors:
    # 如果没有data 直接下一个
    if 'cArticle_view' in each_author:
        if 'rank' in each_author:
            rank = each_author['rank']
            rank['articleViewRank'] = i
        else:
            rank = {
                'articleViewRank': i
            }
        i += 1
        coll.update_one({'mid': each_author['mid']}, {
            '$set': {
                'rank': rank,
            }
        })
    pass

logger.info("计算作者排名结束")
