"""
author: zhr
处理和分解Watdiv文件
"""
import re


def parse_csv(num):
    f = open("D:\\research_data\\watdiv_2g\\bin\\Release\\factor" + str(num) + ".nt", "r", encoding="utf8")
    fw = open("D:\\research_data\\watdiv_2g\\bin\\Release\\factor" + str(num) + ".csv", "w", encoding="utf8")
    line = f.readline()
    while line:
        result = re.search("([^\t]*)\t([^\t]*)\t([^\t\r\n]*) \\.", line)
        if result:
            s = result.group(1)
            p = result.group(2)
            o = result.group(3)
            fw.write(s + "\t" + p + "\t" + o + "\n")
        line = f.readline()
    f.close()
    fw.close()


def count(num):
    f = open("D:\\research_data\\watdiv_2g\\bin\\Release\\factor" + str(num) + ".nt", "r", encoding="utf8")
    line = f.readline()
    count = 0
    while line:
        count += 1
        line = f.readline()
    print(count)
    f.close()


if __name__ == '__main__':
    count(30)
    count(60)
    count(90)
    count(120)
    count(150)
