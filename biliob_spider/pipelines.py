# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
from db import settings
from db import mysql_connect
import datetime
import logging
import redis
from db import redis_connect_string
from bson import ObjectId


def sentCallBack(object_id, coll):
    if object_id != None:
        coll.update_one({'_id': ObjectId(object_id)}, {
            '$set': {'isExecuted': True}})


class StrongPipeline(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.redis_connection = redis.from_url(redis_connect_string)

    def process_item(self, item, spider):
        try:
            self.coll = self.db['video']
            self.coll.update_one({
                'aid': int(item['aid'])
            }, {
                '$set': {
                    'cView': item['current_view'],
                    'cFavorite': item['current_favorite'],
                    'cDanmaku': item['current_danmaku'],
                    'cCoin': item['current_coin'],
                    'cShare': item['current_share'],
                    'cLike': item['current_like'],
                    'cDatetime': item['current_datetime'],
                    'author': item['author'],
                    'subChannel': item['subChannel'],
                    'channel': item['channel'],
                    'mid': item['mid'],
                    'pic': item['pic'],
                    'title': item['title'],
                    'datetime': datetime.datetime.fromtimestamp(
                        item['datetime'])
                },
                '$push': {
                    'data': {
                        '$each': [item['data_video']],
                        '$position': 0
                    }
                }
            }, True)
            # 刷新redis数据缓存
            self.redis_connection.delete(
                "video_detail::{}".format(item['aid']))
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))
        try:
            self.coll = self.db['author']  # 获得collection的句柄
            self.coll.update_one({
                'mid': item['mid']
            }, {
                '$set': {
                    'focus': True,
                    'sex': item['sex'],
                    'name': item['name'],
                    'face': item['face'],
                    'level': item['level'],
                    'cFans': item['c_fans'],
                    'official': item['official'],
                    'cArchive': item['c_archive'],
                    'cArticle': item['c_article'],
                    'cAttention': item['c_attention'],
                    'cArchive_view': item['c_archive_view'],
                    'cArticle_view': item['c_article_view'],
                },
                '$push': {
                    'data': {
                        '$each': [item['data_author']],
                        '$position': 0
                    }
                }
            }, True)
            self.redis_connection.delete(
                "author_detail::{}".format(item['mid']))
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))
        return item


class VideoPipeline(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['video']  # 获得collection的句柄
        self.redis_connection = redis.from_url(redis_connect_string)

    def process_item(self, item, spider):
        try:
            self.coll.update_one({
                'aid': int(item['aid'])
            }, {
                '$set': {
                    'cView': item['current_view'],
                    'cFavorite': item['current_favorite'],
                    'cDanmaku': item['current_danmaku'],
                    'cCoin': item['current_coin'],
                    'cShare': item['current_share'],
                    'cLike': item['current_like'],
                    'cDatetime': item['current_datetime'],
                    'author': item['author'],
                    'subChannel': item['subChannel'],
                    'channel': item['channel'],
                    'mid': item['mid'],
                    'pic': item['pic'],
                    'title': item['title'],
                    'datetime': datetime.datetime.fromtimestamp(
                        item['datetime'])
                },
                '$push': {
                    'data': {
                        '$each': [item['data']],
                        '$position': 0
                    }
                }
            }, True)
            if 'object_id' in item:
                sentCallBack(item['object_id'], self.db['user_record'])
            # self.redis_connection.delete(
            #     "video_detail::{}".format(item['aid']))
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))


class VideoPipelineFromKan(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['video']  # 获得collection的句柄

    def process_item(self, item, spider):
        try:
            self.coll.update_one({
                'aid': int(item['aid'])
            }, {
                '$set': {
                    'author': item['author'],
                    'mid': item['mid'],
                    'pic': item['pic'],
                    'title': item['title'],
                    'datetime': item['datetime']
                },
                '$push': {
                    'data': {
                        '$each': [item['data']],
                        '$position': 0
                    }
                }
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))


class BangumiAndDonghuaPipeLine(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄

    def process_item(self, item, spider):
        try:
            self.coll = self.db[item['collection']]  # 获得collection的句柄
            self.coll.update_one({
                'title': item['title']
            }, {
                '$set': {
                    'title': item['title'],
                    'cover': item['cover'],
                    'newest': item['newest_ep_index'],
                    'currentPts': item['data']['pts'],
                    'currentPlay': item['data']['play'],
                    'currentWatch': item['data']['watch'],
                    'currentReview': item['data']['review'],
                    'currentDanmaku': item['data']['danmaku']
                },
                '$addToSet': {
                    'data': item['data']
                }
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))


class BangumiPipeLine(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['bangumi']  # 获得collection的句柄

    def process_item(self, item, spider):
        try:
            self.coll.update_one({
                'title': item['title']
            }, {
                '$set': {
                    'title': item['title'],
                    'cover': item['cover'],
                    # 'isFinish': item['is_finish'],
                    # 'isStarted': item['is_started'],
                    'newest': item['newest_ep_index'],
                    'currentPts': item['data']['pts'],
                    'currentPlay': item['data']['play'],
                    # 'squareCover': item['square_cover'],
                    'currentWatch': item['data']['watch'],
                    'currentReview': item['data']['review'],
                    'currentDanmaku': item['data']['danmaku']
                },
                '$addToSet': {
                    'data': item['data']
                }
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))


class DonghuaPipeLine(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['donghua']  # 获得collection的句柄

    def process_item(self, item, spider):
        try:
            self.coll.update_one({
                'title': item['title']
            }, {
                '$set': {
                    'title': item['title'],
                    'cover': item['cover'],
                    # 'isFinish': item['is_finish'],
                    # 'isStarted': item['is_started'],
                    'newest': item['newest_ep_index'],
                    'currentPts': item['data']['pts'],
                    'currentPlay': item['data']['play'],
                    # 'squareCover': item['square_cover'],
                    'currentWatch': item['data']['watch'],
                    'currentReview': item['data']['review'],
                    'currentDanmaku': item['data']['danmaku']
                },
                '$addToSet': {
                    'data': item['data']
                }
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))


class SiteInfoPipeline(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['site_info']  # 获得collection的句柄

    def process_item(self, item, spider):
        try:
            self.coll.insert_one({
                'region_count': item['region_count'],
                'all_count': item['all_count'],
                'web_online': item['web_online'],
                'play_online': item['play_online'],
                'datetime': datetime.datetime.now()
            })
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))


class AuthorPipeline(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['author']  # 获得collection的句柄
        self.redis_connection = redis.from_url(redis_connect_string)

    def process_item(self, item, spider):
        try:
            self.coll.update_one({
                'mid': item['mid']
            }, {
                '$set': {
                    'focus': True,
                    'sex': item['sex'],
                    'name': item['name'],
                    'face': item['face'],
                    'level': item['level'],
                    'cFans': item['c_fans'],
                    'official': item['official'],
                },
                '$push': {
                    'data': {
                        '$each': [item['data']],
                        '$position': 0
                    }
                }
            }, True)
            if 'object_id' in item:
                sentCallBack(item['object_id'], self.db['user_record'])
            # self.redis_connection.delete(
            #     "author_detail::{}".format(item['mid']))
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))


class OnlinePipeline(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['video_online']  # 获得collection的句柄

    def process_item(self, item, spider):
        try:

            self.coll.update_one({
                'title': item['title']
            }, {
                '$set': {
                    'title': item['title'],
                    'author': item['author'],
                    'channel': item['channel'],
                    'subChannel': item['subChannel'],
                },
                '$addToSet': {
                    'data': item['data']
                }
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))


class TagPipeLine(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['tag']  # 获得collection的句柄

    def process_item(self, item, spider):
        try:

            self.coll.update_one({
                'tag_id': item['tag_id']
            }, {
                '$set': {
                    'tag_name': item['tag_name'],
                    'ctime': item['ctime'],
                },
                '$addToSet': {
                    'use': item['use'],
                    'atten': item['atten'],
                    'datetime': datetime.datetime.now()
                }
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))


class VideoAddPipeline(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['video']  # 获得collection的句柄

    def process_item(self, item, spider):
        try:
            for each_aid in item['aid']:
                self.coll.update_one({
                    'aid': each_aid
                }, {
                    '$set': {
                        'aid': each_aid,
                        'focus': True
                    },
                }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))


class AuthorChannelPipeline(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['author']  # 获得collection的句柄
        self.redis_connection = redis.from_url(redis_connect_string)

    def process_item(self, item, spider):
        try:
            self.coll.update_one({
                'mid': item['mid']
            }, {
                '$set': {
                    'channels': item['channels']
                },
            }, True)
            self.redis_connection.delete(
                "author_detail::{}".format(item['mid']))
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))


class BiliMonthlyRankPipeline(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['monthly_rank']  # 获得collection的句柄

    def process_item(self, item, spider):
        try:
            self.coll.update_one({
                'aid': item['aid']
            }, {
                '$addToSet': {
                    'pts': item['pts'],
                    'datetime': datetime.datetime.now()
                },
                '$set': {
                    'title': item['title'],
                    'author': item['author'],
                    'aid': item['aid'],
                    'mid': item['mid'],
                    'channel': item['channel'],
                    'currentPts': item['pts']
                }
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error('{}: {}'.format(spider.name, error))
