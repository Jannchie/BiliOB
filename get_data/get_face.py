from face import face
from color import color
from haishoku.haishoku import Haishoku
import csv
from db import cursor
from db import db
s = set()
c = csv.reader(open('./origin_data.csv',
                    'r', encoding="utf-8-sig"))
next(c)
for each in c:
    s.add(each[0])

with open('./get_data/face.py', 'w', encoding="utf-8-sig") as f:
    f.writelines('face = ' + str(face))

authors = []
for each_author in s:
    authors.append(each_author)
    if each_author not in face:

        # cursor.execute(""" SELECT * FROM author WHERE name=%s """, each_author)
        author_data = db['author'].find_one({'name': each_author}, {'face': 1})
        face[each_author] = author_data['face']
        pass
with open('./get_data/face.py', 'w', encoding="utf-8-sig") as f:
    f.writelines('face = ' + str(face))


def get_color_list(path):
    try:
        return Haishoku.getPalette(path)
    except Exception:
        return get_color_list(path)


for each_author in face:
    if each_author in color:
        continue
    if face[each_author][-3:] == 'gif' or each_author == '开眼视频App':
        color[each_author] = '#000000'
    else:

        color_list = get_color_list(face[each_author])
        color_list = sorted(
            color_list, key=lambda x: x[1][0] + x[1][1] + x[1][2])
        color[each_author] = 'rgb' + \
            str(color_list[int(len(color_list)/2)][1])

with open('./get_data/color.py', 'w', encoding="utf-8-sig") as f:
    f.writelines('color = ' + str(color))
