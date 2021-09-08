# -*- coding: utf-8 -*-
# @Time  : 2021/3/7 16:23
# @Author: Jennie Qi
# @File  : temp.py
'''
This file is for temporary coding. It may be unnecessary.
'''
import random
import re


def workload_wash1():
    with open("/home/cuda/qzx/lubm_data/University0-7.nt", "r") as f:
        lines = f.readlines()
    with open("/home/cuda/qzx/lubm_data/University0-7_without_dots.nt", "w") as f:
        for line in lines:
            objs = line.strip().split()
            if False:  # True: 带尖括号; False: 不带尖括号
                for i in range(len(objs)):
                    objs[i] = objs[i][1:-1]

            f.write(" ".join(objs[:3]))
            f.write("\n")


def workload_wash2():
    with open("../res/lubm/workload_sparql_0-7.txt", "r", encoding="utf8") as f:
        sparql_list = re.findall("SELECT.*?\{.*?\}", f.read(), re.S)

    for i in range(len(sparql_list)):
        sparql_list[i] = sparql_list[i].replace("rdf:type",
                                                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")
        ub_search = re.findall("\\s+ub:(.*?)\\s+", sparql_list[i], re.M)
        for item in ub_search:
            print(item)
            sparql_list[i] = sparql_list[i].replace("ub:" + item,
                                                    "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#" + item + ">")
    with open("../res/lubm/workload_sparql_0-7_.txt", "w", encoding='utf8') as f:
        for sparql in sparql_list:
            f.write(sparql)
            f.write("\n")
            f.write("\n")


def data_aggregate():
    target_file = open("C:\\Users\\Ms Rabbit\\Desktop\\demo_video\\lubm_data\\systhesis\\University0-14.nt", "w",
                       encoding='utf8')
    for i in range(15):
        with open("C:\\Users\\Ms Rabbit\\Desktop\\demo_video\\lubm_data\\University{0}.nt".format(i), "r") as f:
            lines = f.readlines()
        for line in lines:
            _ = line.strip().split()
            target_file.write(" ".join(_[:3]))
            target_file.write("\n")


def data_from_sample():
    target_file = open("../res/yago/yago150w.txt", "w", encoding='utf8')
    with open("../res/yago/yago1000w_without_labels.txt", "r", encoding='utf8') as f:
        lines = f.readlines()
    sample_results = random.sample(lines, 1500000)
    for line in sample_results:
        target_file.write(line)


def query_sample(times):
    with open("../res/yago/workload_sparql_x10", "r") as f:
        sparql_list = re.findall("SELECT.*?\{.*?\}", f.read(), re.S)
    split_sparql_list = [list() for i in range(4)]
    for i in range(len(sparql_list)):
        split_sparql_list[i // 10].append(sparql_list[i])
    i = 1
    with open("../res/yago/workload_sparql_x{0}".format(times), "w") as f:
        # f.write("##### {0}".format(i))
        f.write("\n")
        f.write("\n")
        for each_list in split_sparql_list:
            print(len(each_list))
            for each_sample in random.sample(each_list, times):
                f.write(each_sample)
                f.write("\n")
                f.write("\n")


def data_wash():
    with open("../res/watdiv/watdiv100.txt", "r") as f:
        lines = f.readlines()
    with open("../res/watdiv/watdiv100_.txt", "w") as f:
        for line in lines:
            _ = line.strip().split()
            f.write(" ".join(_[:3]))
            f.write("\n")


def yago_data_wash():
    with open("../res/yago/yagoFacts.tsv", "r", encoding='utf8') as f:
        lines = f.readlines()
    with open("../res/yago/yago1000w.txt", "w", encoding='utf8') as f:
        for line in lines[1:]:
            _ = line.strip().split("\t")
            f.write(" ".join(_[1:4]))
            f.write("\n")


if __name__ == '__main__':
    # data_aggregate()
    data_from_sample()
    # query_sample(9)
    # data_wash()
    # yago_data_wash()
    # with open("../res/yago/yago1000w.txt", "r", encoding='utf8') as f:
    #     lines = f.readlines()
    # with open("../res/yago/yago4000w.txt", "w", encoding='utf8') as f:
    #     for i in range(4):
    #         for line in lines:
    #             f.write(line)
