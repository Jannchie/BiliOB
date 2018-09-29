# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
from db import settings
import datetime
import logging


class VideoPipeline(object):
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
                "aid": int(item["aid"])
            }, {
                "$set": {
                    "author": item['author'],
                    "subChannel": item['subChannel'],
                    "channel": item['channel'],
                    "title": item['title'],
                    "datetime": datetime.datetime.fromtimestamp(
                        item['datetime'])
                },
                "$addToSet": {
                    'data': item['data']
                }
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error(error)


class AuthorPipeline(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['author']  # 获得collection的句柄

    def process_item(self, item, spider):
        try:
            self.coll.update_one({
                "mid": item["mid"]
            }, {
                "$set": {
                    "name": item['name'],
                    "face": item['face'],
                    "official": item['official'],
                    "level": item['level'],
                    "sex": item['sex'],
                },
                "$addToSet": {
                    'data': item['data']
                }
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error(error)
        
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
                "title": item["title"]
            }, {
                "$set": {
                    "title": item['title'],
                    "author": item['author'],
                },
                "$addToSet": {
                    'data': item['data']
                }
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error(error)

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
            self.coll.update_one({
                "aid": item["aid"]
            }, {
                "$set": {
                    "aid": item['aid']
                },
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error(error)

class AuthorChannelPipeline(object):
    def __init__(self):
        # 链接mongoDB
        self.client = MongoClient(settings['MINGO_HOST'], 27017)
        # 数据库登录需要帐号密码
        self.client.admin.authenticate(settings['MINGO_USER'],
                                       settings['MONGO_PSW'])
        self.db = self.client['biliob']  # 获得数据库的句柄
        self.coll = self.db['author']  # 获得collection的句柄

    def process_item(self, item, spider):
        try:
            self.coll.update_one({
                "mid": item["mid"]
            }, {
                "$set": {
                    "channels": item['channels']
                },
            }, True)
            return item
        except Exception as error:
            # 出现错误时打印错误日志
            logging.error(error)
