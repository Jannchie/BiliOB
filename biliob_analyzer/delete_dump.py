from db import db
import functools
coll = db['user']
f = coll.find({"name": {"$exists": True, "$regex": '/^.{25,}$/'}})
for each in f:
    print(each)
