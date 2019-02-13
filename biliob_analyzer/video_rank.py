from db import settings
from db import db
import datetime
import logging
from pymongo import DESCENDING
coll = db['video']  # 获得collection的句柄

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s @ %(name)s: %(message)s')
logger = logging.getLogger(__name__)

logger.info("开始计算视频数据排名")

i = 1

keys = ['cView', 'cLike', 'cDanmaku', 'cFavorite', 'cCoin', 'cShare']
for each_key in keys:
    rank_key = each_key[1:] + 'Rank'
    logger.info("开始计算视频{}排名".format(each_key))
    videos = coll.find({}, {'aid': 1, 'rank': 1, each_key: 1}).batch_size(
        20).sort(each_key, DESCENDING)
    for each_video in videos:
        # 如果没有data 直接下一个
        if each_key in each_video:
            if 'rank' in each_video:
                rank = each_video['rank']
                rank[rank_key] = i
            else:
                rank = {
                    rank_key: i
                }
            i += 1
            coll.update_one({'aid': each_video['aid']}, {
                '$set': {
                    'rank': rank,
                }
            })

    logger.info("完成计算视频数据排名")
