"""
author: zhr
处理和裁剪大数据集文件。
"""
import os
import re
from multiprocessing import Process


def split():
    f = open("/var/lib/mysql-files/dbpedia.csv", "r", encoding="utf8")
    line = f.readline()
    p_count = dict()
    num_count = dict()
    count = 0
    while line:
        result = re.search("([^\t]*)\t([^\t]*)\t([^\t]*)", line)
        if result:
            p = result.group(2)
            if p in p_count:
                num_count[p] = num_count[p] + 1
                with open("/var/lib/mysql-files/depedia_" + str(p_count[p]) + ".csv", "a", encoding="utf8") as fw:
                    fw.write(line)
            else:
                print("A New Predicate:" + p)
                p_count[p] = len(p_count) + 1
                num_count[p] = 1
                with open("/var/lib/mysql-files/depedia_" + str(p_count[p]) + ".csv", "a", encoding="utf8") as fw:
                    fw.write(line)
        line = f.readline()
        count += 1
        if count % 1000000 == 0:
            print(count)
            print(line)
    print(p_count)
    print(len(p_count))
    with open("p_count.txt", "w", encoding="utf8") as fw:
        for p in p_count:
            fw.write(p + "\t" + str(p_count[p]) + "\n")
    with open("dbpedia_p_record.txt", "w", encoding="utf8") as fw:
        for p in num_count:
            fw.write(p + "\t" + str(num_count[p]) + "\n")
    f.close()


def split_half():
    f = open("dbpedia.nt", "r", encoding="utf8")
    line = f.readline()
    for i in range(8):
        count = 0
        with open("dbpedia/data/dbpedia" + str(i) + ".nt", "w", encoding="utf8") as fw:
            while line:
                fw.write(line)
                count += 1
                line = f.readline()
                if count % 1000000 == 0:
                    print(count)
                if count >= 32500000:
                    break
    f.close()


def count_max():
    f = open("D:\\research_data\\data9\\LGD-Dump-110406-NodePositions.sorted.nt", "r", encoding="utf8")
    line = f.readline()
    s_max_len = 0
    o_max_len = 0
    p_max_len = 0
    count = 0
    count2 = 0
    while line:
        result = re.search("([<|\"][^<>\"]*[>|\"])	([<|\"][^<>\"]*[>|\"])	([<|\"][^<>\"]*[>|\"]) \\.", line)
        if result:
            count2 += 1
            if len(result.group(1)) > s_max_len:
                s_max_len = len(result.group(1))
                print("New S:" + result.group(1))
            if len(result.group(2)) > p_max_len:
                p_max_len = len(result.group(2))
                print("New P:" + result.group(2))
            if len(result.group(3)) > o_max_len:
                o_max_len = len(result.group(3))
                print("New O:" + result.group(3))
        line = f.readline()
        count += 1
        if count % 1000000 == 0:
            print(count)
            print(count2)
            print("s" + str(s_max_len))
            print("p" + str(p_max_len))
            print("o" + str(o_max_len))
    print("s" + str(s_max_len))
    print("p" + str(p_max_len))
    print("o" + str(o_max_len))


def process_data():
    p_dict = dict()
    print("开始读取p_dict")
    with open("D:\\ChromeDownload\\LGD\\lgd\\lgd_p_count.txt", "r", encoding="utf8") as f:
        line = f.readline()
        while line:
            result = re.search("([^\t]*)\t([^\t\n]*)", line)
            p_dict[result.group(1)] = result.group(2)
            line = f.readline()
    print("读取p_dict完成")
    for i in range(8):
        p = Process(target=_thread_process, args=("D:\\ChromeDownload\\LGD\\lgd\\data\\lgd_" + str(i) + ".nt", p_dict, i))
        p.start()
        print("运行: Thread  " + str(i))


def _thread_process(path, p_dict, thread_num):
    f = open(path, "r", encoding="utf8")
    line = f.readline()
    count = 0
    while line:
        result = re.search("([<|\"][^<>\"]*[>|\"]) ([<|\"][^<>\"]*[>|\"]) ([<|\"][^<>\"]*[>|\"])", line)
        if result:
            s = result.group(1)[:500]
            p = result.group(2)[:500]
            o = result.group(3)[:500].replace("\"", "")
            with open("D:\\ChromeDownload\\LGD\\lgd\\data\\threads\\" + str(thread_num) + "\\depedia_" + str(p_dict[p]) + ".csv", "a", encoding="utf8") as fw:
                    fw.write(s + "\t" + p + "\t" + o + "\n")
        line = f.readline()
        count += 1
        if count % 100000 == 0:
            print(count)
            print(line)
    f.close()


def merge():
    for i in range(8):
        files = os.listdir("dbpedia/threads/" + str(i))
        for file in files:
            with open("dbpedia/threads/" + str(i) + "/" + file, "r", encoding="utf8") as f:
                with open("dbpedia/threads/merge/" + file, "a", encoding="utf8") as fw:
                    print("Start Merge:" + file)
                    line = f.readline()
                    count = 0
                    while line:
                        fw.write(line)
                        count += 1
                        line = f.readline()
                        if count % 1000000 == 0:
                            print(file + ":" + str(count))

"""
以下用于处理LGD文件，由于其分隔符是\t，因此比较好分
"""
def get_p_dict():
    f = open("D:\\ChromeDownload\\LGD\\lgd\\lgd_ct.nt", "r", encoding="utf8")
    line = f.readline()
    p_dict = dict()
    num = 0
    while line:
        split = line.split("\t")
        p = split[1]
        if p not in p_dict:
            p_dict[p] = num
            num += 1
        line = f.readline()
    with open("D:\\ChromeDownload\\LGD\\lgd\\lgd_p_count.txt", "w", encoding="utf8") as fw:
        for p in p_dict:
            fw.write(p + "\t" + str(p_dict[p]) + "\n")

def split_lgd():
    p_dict = dict()
    print("开始读取p_dict")
    with open("D:\\ChromeDownload\\LGD\\lgd\\lgd_p_count.txt", "r", encoding="utf8") as f:
        line = f.readline()
        while line:
            result = re.search("([^\t]*)\t([^\t\n]*)", line)
            p_dict[result.group(1)] = result.group(2)
            line = f.readline()
    print("读取p_dict完成")
    f = open("D:\\ChromeDownload\\LGD\\lgd\\lgd_ct.nt", "r", encoding="utf8")
    line = f.readline()
    count = 0
    p_lines = dict()
    for p in p_dict:
        p_lines[p_dict[p]] = []
    while line:
        result = line.replace("\\", "").replace("\"", "").replace("'", "").replace("\r", "").replace("\n", "").split("\t")
        s = result[0][:500]
        p = result[1][:500]
        o = result[2][:500].rstrip(" .")
        p_lines[p_dict[p]].append(s + "\t" + p + "\t" + o + "\n")
        line = f.readline()
        count += 1
        if count % 300000 == 0:
            print(count)
            print(line)
            for p in p_lines:
                if len(p_lines[p]) > 0:
                    with open("D:\\ChromeDownload\\LGD\\lgd\\data\\threads\\lgd_" + str(p) + ".csv", "a",
                              encoding="utf8") as fw:
                        for word in p_lines[p]:
                            fw.write(word)
                    p_lines[p] = []
    f.close()


def sort_by_size():
    """第二步，将所有文件按大小排序"""
    p_size = dict()
    for root, dirs, files in os.walk("D:\\ChromeDownload\\LGD\\lgd\\data\\threads"):
        for file in files:
            result = re.search("lgd_([0-9]*)\\.csv", file)
            if result:
                number = int(result.group(1))
                p_size[number] = os.path.getsize("D:\\ChromeDownload\\LGD\\lgd\\data\\threads\\" + file)
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
    with open("D:\\ChromeDownload\\LGD\\lgd\\data\\lgd_p_sum.txt", "w", encoding="utf8") as f:
        for pointer in p_sum:
            f.write(str(pointer) + "\t" + str(p_sum[pointer]) + "\t" + str(p_sum_list[pointer]) + '\n')

    _sum_file(p_sum_list)


def _sum_file(p_sum_list):
    for pointer in p_sum_list:
        with open("D:\\ChromeDownload\\LGD\\lgd\\data\\merge\\lgd_sum_" + str(pointer) + ".csv", "w", encoding="utf8") as fw:
            for num in p_sum_list[pointer]:
                with open("D:\\ChromeDownload\\LGD\\lgd\\data\\threads\\lgd_" + str(num[0]) + ".csv", "r", encoding="utf8") as f:
                    line = f.readline()
                    while line:
                        fw.write(line)
                        line = f.readline()


def get_p_record(num):
    f = open("D:\\research_data\\watdiv_2g\\bin\\Release\\factor" + str(num) + ".csv", "r", encoding="utf8")
    line = f.readline()
    p_record = dict()
    while line:
        result = line.replace("\\", "").replace("\"", "").replace("'", "").replace("\r", "").replace("\n", "").split(
            "\t")
        p = result[1][:500]
        if p not in p_record:
            p_record[p] = 1
        else:
            p_record[p] += 1
        line = f.readline()
    with open("D:\\research_data\\watdiv_2g\\bin\\Release\\watdiv" + str(num) + "_p_record.txt", "w", encoding="utf8") as fw:
        for p in p_record:
            fw.write(p + "\t" + str(p_record[p]) + "\n")




if __name__ == '__main__':
    # get_p_dict()
    # split_lgd()
    # sum_p(sort_by_size())
    get_p_record(30)
    get_p_record(60)
    get_p_record(90)
    get_p_record(120)
    get_p_record(150)
