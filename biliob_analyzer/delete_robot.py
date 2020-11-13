from db import db
from db import db
from bson import ObjectId
coll = db['user']  # 获得collection的句柄
d = coll.find({
    'favoriteAid': {
        '$exists': False
    }, 
    'favoriteMid': {
        '$exists': False
    }, 
    '_id': {
        '$gt': ObjectId('5c139d1ca3d20a2e31d717f3')
    }
}).batch_size(100)
# for each in d:
#     if len(each['name'])>20:
#         coll.delete_one({'name':each['name']})
#     pass

s = [';','\\',']','[',',','_','<','`','.','\'','!','~','>',':','/',':','#','(',')']
for each in d:
    print(each['name'])
    coll.delete_one({'name':each['name']})
    pass