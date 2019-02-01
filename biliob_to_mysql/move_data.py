from db import cursor
from db import db as mongodb
from pymongo import ASCENDING
import bson
import datetime
mongo_user = mongodb['user']
mongo_video = mongodb['video']
mongo_author = mongodb['author']

# 用户相关

INSERT_USER_SQL = """
INSERT INTO `user` (`name`, `password`, `credit`, `exp`, `gmt_create`, `role`)
VALUES (%(name)s, %(password)s, %(credit)s, %(exp)s, %(gen_time)s, %(role)s)
ON DUPLICATE KEY UPDATE `name` = VALUES(`name`), `exp` = VALUES(`exp`), `credit` = VALUES(`credit`), `password` = VALUES(`password`), `role` = VALUES(`role`);
"""

GET_USER_ID_SQL = """
SELECT `user_id` FROM `user` WHERE `name` = %s
"""

DELETE_USER_FOCUS_VIDEO_SQL = """
DELETE FROM biliob.user_focus_video
WHERE
    `user_id` = %s;
"""

DELETE_USER_FOCUS_AUTHOR_SQL = """
DELETE FROM biliob.user_focus_author
WHERE
    `user_id` = %s;
"""

INSERT_USER_FOCUS_VIDEO_SQL = """
INSERT INTO `user_focus_video` (`user_id`, `video_id`)
VALUES (%(user_id)s, %(video_id)s);
"""

INSERT_USER_FOCUS_AUTHOR_SQL = """
INSERT INTO `user_focus_author` (`user_id`, `author_id`)
VALUES (%(user_id)s, %(author_id)s)
"""


def translate_int64(item):
    for each_key in item:
        if type(item[each_key]) is bson.int64.Int64:
            item[each_key] = int(item[each_key])


def move_user():
    for each_doc in mongo_user.find().sort('_id', direction=ASCENDING):
        item = dict()
        item['gen_time'] = each_doc.pop('_id').generation_time
        item['name'] = each_doc['name']
        item['credit'] = each_doc['credit'] if 'credit' in each_doc else 0
        item['password'] = each_doc['password'] if 'password' in each_doc else 0
        item['exp'] = each_doc['exp'] if 'exp' in each_doc else 0
        item['role'] = each_doc['role'] if 'role' in each_doc else 0
        if len(item['name']) > 45:
            print(item['name'])
            continue
        cursor.execute(INSERT_USER_SQL, item)
        cursor.execute(GET_USER_ID_SQL, (each_doc['name']))

        user_id = cursor.fetchone()['user_id']
        cursor.execute(DELETE_USER_FOCUS_VIDEO_SQL, (user_id))
        cursor.execute(DELETE_USER_FOCUS_AUTHOR_SQL, (user_id))
        if 'favoriteAid' in each_doc:
            for each_aid in each_doc['favoriteAid']:
                if each_aid == None or each_aid > 4294967295:
                    continue
                item = {}
                item['user_id'] = int(user_id)
                item['video_id'] = int(each_aid)
                cursor.execute(INSERT_USER_FOCUS_VIDEO_SQL, item)
        if 'favoriteMid' in each_doc:
            for each_mid in each_doc['favoriteMid']:
                if each_mid == None or each_mid > 4294967295:
                    continue
                item = {}
                item['user_id'] = int(user_id)
                item['author_id'] = int(each_mid)
                cursor.execute(INSERT_USER_FOCUS_AUTHOR_SQL, item)


# 视频相关

INSERT_VIDEO_SQL = """
INSERT INTO `video` (`video_id`, `author_id`, `title`, `pic`, `is_observe`, `gmt_create`, `channel`, `subchannel`, `pub_datetime`)
VALUES (%(video_id)s, %(author_id)s, %(title)s, %(pic)s, %(is_observe)s, %(gen_time)s, %(channel)s, %(subchannel)s, %(pub_datetime)s)
ON DUPLICATE KEY UPDATE `title` = VALUES(`title`), `pic` = VALUES(`pic`), `is_observe` = VALUES(`is_observe`), `channel` = VALUES(`channel`), `subchannel` = VALUES(`subchannel`), `pub_datetime` = VALUES(`pub_datetime`);
"""

INSERT_VIDEO_RECORD_SQL = """
INSERT INTO `video_record` (`video_id`, `view`, `danmaku`, `favorite`, `coin`, `share`, `like`, `dislike`, `gmt_create`)
VALUES (%(video_id)s, %(view)s, %(danmaku)s, %(favorite)s, %(coin)s, %(share)s, %(like)s, %(dislike)s, %(gmt_create)s)
ON DUPLICATE KEY UPDATE 
`video_id` = VALUES(`video_id`), 
`view` = VALUES(`view`), 
`danmaku` = VALUES(`danmaku`), 
`favorite` = VALUES(`favorite`), 
`coin` = VALUES(`coin`), 
`share` = VALUES(`share`);
`like` = VALUES(`like`);
`dislike` = VALUES(`dislike`);
"""


def move_video():
    for each_doc in mongo_video.find().batch_size(8):
        translate_int64(each_doc)
        item = {}
        item['video_id'] = each_doc['aid'] if 'aid' in each_doc else None
        print(item['video_id'])
        item['author_id'] = each_doc['mid'] if 'mid' in each_doc else None
        item['title'] = each_doc['title'] if 'title' in each_doc else None
        item['pic'] = each_doc['pic'] if 'pic' in each_doc else None
        item['is_observe'] = each_doc['focus'] if 'focus' in each_doc else 1
        item['channel'] = each_doc['channel'] if 'channel' in each_doc else None
        item['subchannel'] = each_doc['subChannel'] if 'subChannel' in each_doc else None
        item['gen_time'] = each_doc.pop('_id').generation_time
        item['pub_datetime'] = each_doc['datetime'] if 'datetime' in each_doc else None
        cursor.execute(INSERT_VIDEO_SQL, item)
        if 'data' in each_doc:
            item_list = []
            for each_record in each_doc['data']:
                translate_int64(each_record)
                item = {}
                item['video_id'] = each_doc['aid'] if 'aid' in each_doc else None
                item['view'] = each_record['view'] if 'view' in each_record else None
                item['danmaku'] = each_record['danmaku'] if 'danmaku' in each_record else None
                item['favorite'] = each_record['favorite'] if 'favorite' in each_record else None
                item['coin'] = each_record['coin'] if 'coin' in each_record else None
                item['share'] = each_record['share'] if 'share' in each_record else None
                item['like'] = each_record['like'] if 'like' in each_record else None
                item['dislike'] = each_record['dislike'] if 'dislike' in each_record else None
                item['gmt_create'] = each_record['datetime'] if 'datetime' in each_record else None
                item_list.append(item)
            cursor.executemany(INSERT_VIDEO_RECORD_SQL, item_list)
