from db import db
import functools
coll = db['user']
f = coll.find()
names = set()
for each in f:
    names.add(each['name'])
for each_name in names:
    f = coll.find({'name': each_name})
    while f.count() > 1:
        a= coll.delete_one({
            'name': each_name,
            'favoriteMid': {
                '$exists': False
            },
            'favoriteAid': {
                '$exists': False
            }
        })
        print(a)
        f = coll.find({'name': each_name})
