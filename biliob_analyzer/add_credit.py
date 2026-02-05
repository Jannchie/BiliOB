from db import db
u = db['user']
u.update_many({}, {'$inc': {'credit': 50}})
