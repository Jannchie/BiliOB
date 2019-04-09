# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import datetime
import os
import sys

import redis
from pymongo import MongoClient
from bson import ObjectId
env_dist = os.environ


def sentCallBack(object_id, coll):
    if object_id != None:
        coll.update_one({'_id': ObjectId(object_id)}, {
            '$set': {'isExecuted': True}})


class DanmakuSpiderPipeline(object):
    def __init__(self):
        self.client = MongoClient(env_dist['BILIOB_MONGO_SERVER'], 27017)
        self.client.admin.authenticate(env_dist['BILIOB_MONGO_USER'],
                                       env_dist['BILIOB_MONGO_PASSWD'])
        self.db = self.client['biliob']
        self.redis_connection = redis.from_url(
            env_dist['BILIOB_REDIS_CONNECTION_STRING'])

    def process_item(self, item, spider):
        self.coll = self.db['video']
        self.coll.update_one({
            'aid': int(item['aid'])
        }, {
            '$set': {
                'danmaku_aggregate.{}'.format(item['page_number']): {
                    'duration': item['duration'],
                    'p_name': item['p_name'],
                    'danmaku_density': item['danmaku_density'],
                    'word_frequency': item['word_frequency']
                },
                'danmaku_aggregate.updatetime': datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
            }
        }, True)
        sentCallBack(item['object_id'], self.db['user_record'])
        # # 刷新redis数据缓存
        # self.redis_connection.delete(
        #     "video_detail::{}".format(item['aid']))
