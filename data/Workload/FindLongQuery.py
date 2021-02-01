"""
author: zhr
查找有关查询。
"""
import re


def find():
    f = open("D:\\research_data\\bio2rdf_8g\\bio2rdf2.csv", "r", encoding="utf8")
    line = f.readline()
    s_dict = dict()
    p_dict = dict()
    o_dict = dict()
    while line:
        line = line.replace("\n", "").replace(" ", "").replace("\r", "")
        result = re.search("([^\t ]*)\t([^\t ]*)\t([^\t\n ]*)", line)
        if result:
            s = result.group(1)
            p = result.group(2)
            o = result.group(3)
            if s in s_dict:
                s_dict[s] += 1
            else:
                s_dict[p] = 1
            if p in p_dict:
                p_dict[p] += 1
            else:
                p_dict[p] = 1
            if o in o_dict:
                o_dict[o] += 1
            else:
                o_dict[o] = 1
        line = f.readline()
    f.close()
    return p_dict, o_dict, s_dict


if __name__ == '__main__':
    p_dict, o_dict, s_dict = find()
    print(sorted(s_dict.items(), key=lambda item: item[1], reverse=True)[:10])
    print(sorted(p_dict.items(), key=lambda item: item[1], reverse=True)[:10])
    print(sorted(o_dict.items(), key=lambda item: item[1], reverse=True)[:10])
