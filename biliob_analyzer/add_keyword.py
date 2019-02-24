from pymongo import ReturnDocument
import jieba
from db import db

# 载入字典
jieba.load_userdict('./biliob_analyzer/dict.txt')


class AddKeyword():

    def __init__(self):
        self.mongo_author = db['author']
        self.mongo_video = db['video']

    def get_video_kw_list(self, aid):
        # 关键字从name和official中提取
        video = self.mongo_video.find_one(
            {'aid': aid}, {'_id': 0, 'title': 1, 'channel': 1, 'subChannel': 1, 'author': 1, 'tag': 1})
        kw = []
        for each_key in video:
            if each_key != 'keyword' or each_key != 'tag':
                kw.append(str(video[each_key]).lower())
            elif each_key == 'tag':
                kw += video['tag']
            else:
                kw += video['keyword']
        seg_list = jieba.lcut_for_search(
            ' '.join(kw), True)  # 搜索引擎模式

        # 全名算作关键字
        if 'author' in video and video['author'].lower() not in seg_list:
            seg_list.append(video['author'].lower())

        while ' ' in seg_list:
            seg_list.remove(' ')
        while '、' in seg_list:
            seg_list.remove('、')
        return list(set(seg_list))

    def add_to_video(self, aid, seg_list):
        self.mongo_video.update_one({'aid': aid}, {'$set': {
            'keyword': seg_list
        }})

    def add_video_kw(self, aid):
        self.add_to_video(aid, self.get_video_kw_list(aid))
        return True

    def get_author_kw_list(self, mid):
        # 关键字从name和official中提取
        author = self.mongo_author.find_one(
            {'mid': mid}, {'_id': 0, 'name': 1, 'official': 1, 'keyword': 1})
        kw = []
        for each_key in author:
            if each_key != 'keyword':
                kw.append(str(author[each_key]).lower())
            else:
                kw += author['keyword']
        seg_list = jieba.lcut_for_search(
            ' '.join(kw), True)  # 搜索引擎模式

        # 全名算作关键字
        if 'name' in author and author['name'].lower() not in seg_list:
            seg_list.append(author['name'].lower())

        while ' ' in seg_list:
            seg_list.remove(' ')
        while '、' in seg_list:
            seg_list.remove('、')
        return list(set(seg_list))

    def add_author_kw(self, mid):
        self.add_to_author(mid, self.get_author_kw_list(mid))
        return True

    def add_to_author(self, mid, seg_list):
        self.mongo_author.update_one(
            {'mid': mid}, {'$set': {'keyword': seg_list}})

    def add_all_author(self):
        authors = self.mongo_author.find(
            {'keyword': {'$exists': False}}, {'_id': 0, 'mid': 1})
        for each_author in authors:
            mid = each_author['mid']
            self.add_author_kw(mid)

    def add_all_video(self):
        videos = self.mongo_video.find(
            {'keyword': {'$exists': False}}, {'_id': 0, 'aid': 1})
        for each_video in videos:
            aid = each_video['aid']
            self.add_video_kw(aid)
