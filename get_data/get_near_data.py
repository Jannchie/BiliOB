import csv
c = csv.reader(open('./origin_data.csv', 'r', encoding='utf-8-sig'))
next(c)
d = {}
for each in c:
    data_zip = (each[0], each[2])
    if each[1] not in d:
        d[each[1]] = [data_zip]
    else:
        d[each[1]].append(data_zip)
    pass
d[list(d.keys())[0]].sort(key=lambda x: float(x[1]))
del_list = []
l_value = 0
for each in d[list(d.keys())[0]]:
    c_value = float(each[1])
    print(c_value - l_value)
    if l_value != 0 and (c_value - l_value) < 2500 and c_value < 80 * 10000:
        del_list.append(each[0])
        continue
    l_value = c_value


name_list = ['TheRock_Johnson', 'Jannchie见齐']
with open('./filted.csv', 'w', encoding="utf-8-sig") as f:
    f.write('"name","value","date"\n')
    for each_date in d:
        if each_date < '2019-08-16 22:19:12':
            continue
        d[each_date].sort(key=lambda x: float(x[1]), reverse=True)
        l = len(d[each_date])
        temp = []
        i = 0
        for index in range(l):
            item = d[each_date][index]
            if (item[0] in name_list):
                f.write('"{}","{}","{}"\n'.format(
                    item[0], item[1], each_date))
            if d[each_date][index][0] == 'TheRock_Johnson':
                count = 0
                for each_idx in range(index - 2, l):
                    item = d[each_date][each_idx]
                    if(item[0] not in name_list and item[0] not in del_list):
                        count += 1
                        f.write('"{}","{}","{}"\n'.format(
                            item[0], item[1], each_date))
                    if count == 30:
                        break
