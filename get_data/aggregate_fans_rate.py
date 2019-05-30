from db import db
import datetime
from scipy.interpolate import interp1d
from haishoku.haishoku import Haishoku

from face import face
from color import color

start_date = datetime.datetime(2018, 12, 1)
end_date = datetime.datetime.now()
date_range = 30 * 24 * 60 * 60
delta_date = 0.25 * 24 * 60 * 60
date_format = '%Y-%m-%d %H:%M'
d = {}
output_file = 'D:/数据/B站/fans/月结播放.csv'
field = 'cArchive_view'
field_name = 'archiveView'

nl = ["荒草何茫茫", "观察者网", "敬汉卿", "徐大虾咯", "火播君", "凤凰天使TSKS韩剧社官方账号", "这奇闻有毒",
        "哔哩哔哩弹幕网", "环球时报", "美食作家王刚R", "王咩阿", "星际老男孩", "中国BOY超级大猩猩",
        "旭旭姥姥6868", "花花与三猫CatLive", "徐大sao", "共青团中央", "木鱼水心", "LexBurner", "爱笑兄弟连",
        "哔哩哔哩英雄联盟赛事", "瞬间爆炸型LowSing", "信誓蛋蛋", "故事王StoryMan",
        "老番茄", "天天卡牌", "华农兄弟", "迷影社", "魔力科学小实验", "班里课代表", "北京若森数字",
        "bilibili电影", "山下智博", "我是郭杰瑞", "翔翔大作战", "今日份的沙雕", "江湖百晓生002",
        "黑镖客梦回", "10后找人带", "游泳的猪头", "桃璎Miku", "指法芬芳张大仙", "努力的Lorre",
        "喵不磕盐", "搞笑课代表", "小可儿", "腾讯动漫", "哔哩哔哩纪录片", "pcyxjy", "大胸肌的罗丽",
        "DELA_P", "永无叙", "落纸生花创意手工", "敖厂长", "丝血反杀闰土的猹"
        ]

current_date = start_date.timestamp()
while (current_date < end_date.timestamp()):
    c_date = datetime.datetime.fromtimestamp(current_date).strftime(
        date_format)
    d[c_date] = []
    current_date += delta_date

# for each_author in db['author'].find({each: {'$gt': 200000}}).batch_size(1):
for each_name in nl:
    each_author = db['author'].find_one({'name': each_name})
    
    current_date = start_date.timestamp()

    data = sorted(each_author['data'], key=lambda x: x['datetime'])

    def get_date(each_data):
        if field_name in each_data and each_data[field_name] != 0 and each_data[field_name] != -1:
            return each_data['datetime'].timestamp()

    def get_value(each_data):
        if field_name in each_data and each_data[field_name] != 0 and each_data[field_name] != -1:
            return each_data[field_name]
    px = list(i for i in map(get_date, data) if i != None)
    py = list(i for i in map(get_value, data) if i != None)
    x = []
    y = []
    for i in range(len(px)):
        if i != 0 and py[i] == py[i - 1]:
            continue
        else:
            x.append(px[i])
            y.append(py[i])
        pass
    
    if len(x) <= 2:
        continue
    interrupted_fans = interp1d(x, y, kind='linear')
    current_date = start_date.timestamp()
    while (current_date < min(end_date.timestamp(), x[-1])):
        begin_date = current_date - date_range
        if begin_date <= x[0]:
            begin_date = x[0]
        # 出界
        if begin_date >= x[0] and current_date < x[-1] and current_date > x[0]:
            fans_func = interrupted_fans([begin_date, current_date])
            delta_fans = int(fans_func[1] - fans_func[0])
            pass
            c_date = datetime.datetime.fromtimestamp(current_date).strftime(
                date_format)
            print('{}\t"{}\t{}'.format(each_author['name'], delta_fans,
                                            c_date))
            # d[c_date].append((delta_fans", each_author['name']))

            d[c_date].append((each_author['name'], delta_fans,
                              each_author['face']))

            if len(d[c_date]) >= 200:
                d[c_date] = sorted(
                    d[c_date], key=lambda x: x[1], reverse=True)[:20]
        current_date += delta_date
for c_date in d:
    d[c_date] = sorted(d[c_date], key=lambda x: x[1], reverse=True)[:20]

with open(output_file, 'w', encoding="utf-8-sig") as f:
    f.writelines('date",name",value\n')
    for each_date in d:
        for each_data in d[each_date]:
            f.writelines('"{}","{}","{}"\n'.format(each_date, each_data[0],
                                                   each_data[1]))
authors = []
for each_date in d:
    for each_author in d[each_date]:
        authors.append(each_author[0])
        if each_author[0] not in face:
            face[each_author[0]] = each_author[2]
with open('./get_data/face.py', 'w', encoding="utf-8-sig") as f:
    f.writelines('face = ' + str(face))

for each_author in face:
    if each_author in color:
        continue
    if face[each_author][-3:] == 'gif' or each_author == '开眼视频App':
        color[each_author] = '#000000'
    else:
        color_list = Haishoku.getPalette(face[each_author])
        color_list = sorted(
            color_list, key=lambda x: x[1][0] + x[1][1] + x[1][2])
        color[each_author] = 'rgb' + \
            str(color_list[int(len(color_list)/2)][1])

with open('./get_data/color.py', 'w', encoding="utf-8-sig") as f:
    f.writelines('color = ' + str(color))

min_fans = 99999999
for each_author in authors:
    c_fans = db['author'].find_one({'name': each_author},
                                   {field: True})[field]
    if c_fans <= min_fans:
        min_fans = c_fans
print(min_fans)
