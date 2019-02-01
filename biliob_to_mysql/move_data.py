from db import cursor
from db import db as mongodb
from pymongo import ASCENDING

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
