from pymongo import ReturnDocument
import jieba
from db import db
from time import sleep
# 载入字典


class KeywordAdder():

    def __init__(self):
        self.mongo_author = db['author']
        self.mongo_video = db['video']
        self.mongo_word = db['search_word']
        jieba.load_userdict('./biliob_analyzer/dict.txt')

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
        sleep(0.01)
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
        sleep(0.01)
        self.mongo_author.update_one(
            {'mid': mid}, {'$set': {'keyword': seg_list}})

    def add_all_author(self):
        authors = self.mongo_author.find(
            {
                '$or': [
                    {
                        'keyword': []
                    }, {
                        'keyword': {
                            '$exists': False
                        }
                    }
                ]
            }, {'_id': 0, 'mid': 1}).batch_size(200)
        for each_author in authors:
            mid = each_author['mid']
            self.add_author_kw(mid)

    def add_all_video(self):
        videos = self.mongo_video.find({
            '$or': [
                {
                    'keyword': []
                }, {
                    'keyword': {
                        '$exists': False
                    }
                }
            ]
        }, {'_id': 0, 'aid': 1}).batch_size(200)
        for each_video in videos:
            aid = each_video['aid']
            self.add_video_kw(aid)

    def refresh_all_author(self):
        authors = self.mongo_author.find(
            {}, {'_id': 0, 'mid': 1}).batch_size(500)
        for each_author in authors:
            mid = each_author['mid']
            print("[mid]"+str(mid))
            self.add_author_kw(mid)

    def refresh_all_video(self):
        videos = self.mongo_video.find(
            {}, {'_id': 0, 'aid': 1}).batch_size(500)
        for each_video in videos:
            aid = each_video['aid']
            print("[aid]"+str(aid))
            self.add_video_kw(aid)

    def add_omitted(self):
        if self.mongo_word.count_documents({}) < 100:
            return
        d = open('./biliob_analyzer/dict.txt', 'r',
                 encoding='utf8').read().split('\n')
        for each in self.mongo_word.find():
            if 'aid' in each and each['aid'] not in d:
                d.append(each['aid'])
            elif 'mid' in each and each['mid'] not in d:
                d.append(each['mid'])
            pass
        pass
        o = open('./biliob_analyzer/dict.txt',
                 'w', encoding='utf8', newline='')
        for each in d:
            o.write(each+'\n')
        o.close()
        self.mongo_word.delete_many({})
        jieba.load_userdict('./biliob_analyzer/dict.txt')
        self.refresh_all_author()
        self.refresh_all_video()
