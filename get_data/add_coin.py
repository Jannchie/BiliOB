from db import db

import pymysql
from biliob_scheduler.scheduler import priorityVideoCrawlRequest
mysql_db = pymysql.connect(
    host="localhost",
    user="jannchie",
    port=3306,
    use_unicode=True,
    password="141421",
    db="bilibili",
    charset="utf8mb4",
    autocommit=True)

cursor = mysql_db.cursor(pymysql.cursors.SSCursor)
SQL = """
SELECT
    *
FROM
    bilibili.video
ORDER BY coin DESC
LIMIT 300;
"""

cursor.execute(SQL)

for each in cursor.fetchall():
    aid = each[1]
    priorityVideoCrawlRequest(aid)
    print(aid)
    if (db['video'].find({'aid': aid}).count() == 0):
        print(each[3])
    db['video'].update_one({'aid': aid}, {'$set': {'aid': aid}}, upsert=True)
    pass
