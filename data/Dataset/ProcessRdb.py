"""
用于处理RDB数据，分为以下几步：
1.将所有p分开，统计每一个p所处的文件大小
2.将文件按照大小从小到大排序
3.将文件按照排序聚合成 10个3G左右文件，具体大小不固定，可以超过1次3G，保存下来索引表
4.按照索引表对workload进行分表处理
5.将数据导入MySQL数据库，开始查询
"""
import re
import sys
import os


def split_p():
    """将所有p分开,减少spo的长度到500"""
    f = open("dbpedia2000.nt", "r", encoding="utf8")
    p_dict = dict()  # 保存p的索引文件，格式为p: 索引数字
    p_number = dict()  # 保存每个p有多少元组数的索引文件，格式为p: 数量
    line = f.readline()
    p_count = 0
    line_count = 0
    while line:
        result = re.search("([<|\"][^<>\"]*[>|\"]) ([<|\"][^<>\"]*[>|\"]) ([<|\"][^<>\"]*[>|\"]) \\.", line)
        if result:
            s = result.group(1)
            p = result.group(2)
            o = result.group(3)
            if p not in p_dict:
                p_count += 1
                p_dict[p] = p_count
                p_number[p] = 0
            else:
                p_number[p] += 1
            with open("rdb/dbpedia_" + str(p_dict[p]) + ".csv", "a", encoding="utf8") as fw:
                fw.write(s[:500] + "\t" + p + "\t" + o[:500] + "\n")
        line = f.readline()
        line_count += 1
        if line_count % 100000 == 0:
            print(line_count)
    with open("p_index.txt", "a", encoding="utf8") as fw:
        for p in p_dict:
            fw.write(p + "\t" + str(p_dict[p]) + "\n")
    with open("p_number.txt", "a", encoding="utf8") as fw:
        for p in p_number:
            fw.write(p + "\t" + str(p_dict[p]) + "\n")


def cut():
    """将所有行的列数限制在2000以内"""
    with open("dbpedia.nt", "r", encoding="utf8") as f:
        with open("dbpedia2000.nt", "w", encoding="utf8") as fw:
            count = 0
            line = f.readline()
            while line:
                if len(line) > 5000:
                    fw.write(line[:4992] + line[-8:] + "\n")
                    # print(len(line))
                else:
                    fw.write(line)
                line = f.readline()
                count += 1
                if count % 1000000 == 0:
                    print(count)


def read_p_dict():
    with open("p_index.txt", "a", encoding="utf8") as f:
        p_dict = dict()
        line = f.readline()
        while line:
            result = re.search("([^\t]*)\t([^\t]*)", line)
            if result:
                p_dict[result.group(1)] = int(result.group(2))
            line = f.readline()
        return p_dict


def find_p_by_num(p_dict, num):
    """
    找到和Num数字编号对应的P字符串
    :param p_dict: 存储P和数字编号的dict
    :param num: 编号，需要是int类型
    :return: p，字符串格式
    """
    for p in p_dict:
        if p_dict[p] == num:
            return p


def sort_by_size():
    """第二步，将所有文件按大小排序"""
    p_size = dict()
    for root, dirs, files in os.walk("D:\\PycharmProjects\\tkde\\data\\dbpedia\\threads\\merge"):
        for file in files:
            result = re.search("depedia_([0-9]*)\\.csv", file)
            if result:
                number = int(result.group(1))
                p_size[number] = os.path.getsize("D:\\PycharmProjects\\tkde\\data\\dbpedia\\threads\\merge\\" + file)
    return sorted(p_size.items(), key=lambda item: item[1])  # 对P的大小进行排序


def sum_p(p_size):
    """第三步，将排序后的文件进行聚合"""
    p_sum_list = dict()
    p_sum = dict()
    sum_pointer = 1
    p_sum[sum_pointer] = 0
    p_sum_list[sum_pointer] = []
    for p_tuple in p_size:
        if p_sum[sum_pointer] < 3 * 1000000000 and p_sum[sum_pointer] + int(p_tuple[1]) < 5 * 1000000000:
            p_sum_list[sum_pointer].append(p_tuple)
            p_sum[sum_pointer] += int(p_tuple[1])
        else:
            sum_pointer += 1
            p_sum_list[sum_pointer] = [p_tuple, ]
            p_sum[sum_pointer] = int(p_tuple[1])
    # print(p_sum)
    # print(p_sum_list)
    # print(len(p_sum))
    for p in p_sum_list:
        total = 0
        for tuple in p_sum_list[p]:
            total += int(tuple[1])
        print(total)
    with open("p_sum.txt", "w", encoding="utf8") as f:
        for pointer in p_sum:
            f.write(str(pointer) + "\t" + str(p_sum[pointer]) + "\t" + str(p_sum_list[pointer]) + '\n')

    _sum_file(p_sum_list)


def _sum_file(p_sum_list):
    for pointer in p_sum_list:
        with open("D:\\PycharmProjects\\tkde\\data\\dbpedia\\threads\\second_merge\\depedia_sum_" + str(pointer) + ".csv", "w", encoding="utf8") as fw:
            for num in p_sum_list[pointer]:
                with open("D:\\PycharmProjects\\tkde\\data\\dbpedia\\threads\\merge\\depedia_" + str(num[0]) + ".csv", "r", encoding="utf8") as f:
                    line = f.readline()
                    while line:
                        fw.write(line)
                        line = f.readline()


def add_quote(num):
    num = str(num)
    f = open("dbpedia_sum_" + num + ".csv", "r", encoding="utf8")
    fw = open("dbpedia_sum_" + num + "k.csv", "w", encoding="utf8")
    line = f.readline()
    while line:
        line = re.sub('"', '', line)
        result = re.search("([^\t]*)\t([^\t]*)\t([^\t]*)", line)
        if result:

            fw.write(result.group(1) + "\t" + result.group(2)[:500] + "\t" + result.group(3))
        line = f.readline()
    f.close()
    fw.close()


def parse_p_number():
    p_sum = dict()
    p_index = dict()
    print("1")
    with open("p_sum.txt", "r", encoding="utf8") as fs:
        line = fs.readline()
        while line:
            result = re.search("([0-9]*)\t[0-9]*\t([^\t]*)", line)
            if result:
                all_p = result.group(2).split(",")
                for i in range(0, len(all_p), 2):
                    p_sum[all_p[i].replace("(", "").replace("[", "").strip(" ")] = all_p[i+1].replace(")", "").replace("]", "").strip(" ")
            line = fs.readline()
    print("1")
    with open("p_index.txt", "r", encoding="utf8") as fw:
        line = fw.readline()
        while line:
            result = re.search("([^\t ]*)\t([^\t\n ]*)", line)
            if result:
                p_index[result.group(1)] = result.group(2)
            line = fw.readline()
    print("1")
    p_index_inverse = dict(zip(p_index.values(), p_index.keys()))
    print("1")
    with open("p_number.txt", "w", encoding="utf8") as fs:
        for p in p_sum:
            fs.write(p_index_inverse[p] + "\t"
                     + p_sum[p] + "\n")


def parse():
    f = open("dbpedia.nt", "r", encoding="utf8")
    line = f.readline()
    count = 0
    true_count = 0
    while line:
        result = re.search("([<|\"][^<>\"]*[>|\"]) ([<|\"][^<>\"]*[>|\"]) ([<|\"][^<>\"]*[>|\"])", line)
        count += 1
        if result:
            true_count += 1
        else:
            print(line)
        line = f.readline()
        if count % 100000 == 0:
            print(str(count) + "\t" + str(true_count))
    print("final count:" + str(count))
    print("final true count:" + str(true_count))


def split():
    f = open("D:\\ChromeDownload\\LGD\\lgd\\lgd_ct.nt", "r", encoding="utf8")
    line = f.readline()
    p_count = dict()
    num_count = dict()
    count = 0
    while line:
        result = re.search("([<|\"][^<>\"]*[>|\"])\t([<|\"][^<>\"]*[>|\"])\t([<|\"][^<>\"]*[>|\"])", line)
        if result:
            s = result.group(1)[:500]
            p = result.group(2)[:500]
            o = result.group(3)[:500].replace("\"", "")
            if p in p_count:
                num_count[p] = num_count[p] + 1
                # with open("dbpedia/data/depedia_" + str(p_count[p]) + ".csv", "a", encoding="utf8") as fw:
                #     fw.write(s + "\t" + p + "\t" + o + "\n")
            else:
                print("A New Predicate:" + p)
                p_count[p] = len(p_count) + 1
                num_count[p] = 1
                # with open("dbpedia/data/depedia_" + str(p_count[p]) + ".csv", "a", encoding="utf8") as fw:
                #     fw.write(s + "\t" + p + "\t" + o + "\n")
        line = f.readline()
        count += 1
        if count % 100000 == 0:
            print(count)
            print(line)
            print(p_count)
            print(len(p_count))
    print(p_count)
    print(len(p_count))
    with open("D:\\ChromeDownload\\LGD\\lgd\\lgd_p_count.txt", "w", encoding="utf8") as fw:
        for p in p_count:
            fw.write(p + "\t" + str(p_count[p]) + "\n")
    with open("D:\\ChromeDownload\\LGD\\lgd\\lgd_p_record.txt", "w", encoding="utf8") as fw:
        for p in num_count:
            fw.write(p + "\t" + str(num_count[p]) + "\n")
    f.close()


if __name__ == '__main__':
    # add_quote(2)
    # add_quote(3)
    # add_quote(4)
    with open("new_p_index.txt", "w", encoding="utf8") as f:
        p_size = sort_by_size()
    # total_size = 0
    # for root, dirs, files in os.walk("/home/zhr/data/rdb"):
    #     for file in files:
    #         result = re.search("dbpedia_([0-9]*)\\.csv", file)
    #         if result:
    #             number = int(result.group(1))
    #             total_size += os.path.getsize("/home/zhr/data/rdb/" + file)
    # print(total_size)


