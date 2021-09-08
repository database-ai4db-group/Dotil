# -*- coding: utf-8 -*-
'''
This file is used to construct candidate sets of storages.
'''
import re
import os
import sys
# from itertools import combinations
# from scipy.special import comb

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from src.sparql2sql import Sparql2sql


class StorageConstructor:
    '''
    存储结构枚举器
    '''

    def __init__(self, workload_filepath):
        self.sparql_list = None
        self.workload_filepath = workload_filepath
        with open(self.workload_filepath, "r") as f:
            self.sparql_list = re.findall("SELECT.*?\{.*?\}", f.read(), re.S)


    def extract_predicates_from_star_query(self, sparql):
        '''
        获取sparql中所有星型子查询的p集合
        '''
        # extract triples
        where_clause = re.search("WHERE.*?\{(.*?)\}", sparql, re.S).group(1)
        triples = [x.strip() for x in where_clause.split("\n") if x.strip()]

        temp_dict = dict()
        for each_triple in triples:
            # match [s p o]
            triple_split = re.match("(.*)\\s+(.*)\\s+(.*)\\s+\\.", each_triple).groups()
            if triple_split[1] == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>":
                # ignore "%type>" without object
                if triple_split[2].startswith("?"):
                    continue
                else:
                    temp_dict.setdefault(triple_split[0], set()).add(triple_split[2])
            else:
                temp_dict.setdefault(triple_split[0], set()).add(triple_split[1])
        return temp_dict

    def extract_star_query_from_original(self, sparql, subject, subquery):
        '''
        根据星型子查询的predicates，从原sparql中切割出sub-sparql
        '''
        where_clause = re.search("WHERE.*?\{(.*?)\}", sparql, re.S).group(1)  # where子句
        new_where_clause = where_clause  # where子句副本
        triples = [x.strip() for x in where_clause.split("\n") if x.strip()]
        all_select_items = set()
        for each_triple in triples:
            triple_split = re.match("(.*)\\s+(.*)\\s+(.*)\\s+\\.", each_triple).groups()
            if triple_split[1] not in subquery or triple_split[0] != subject:
                new_where_clause = new_where_clause.replace(each_triple, "")
            else:
                for i in range(3):
                    if triple_split[i].startswith("?"):
                        all_select_items.add(triple_split[i])
        sparql = sparql.replace(where_clause, new_where_clause)
        select_clause = re.search("SELECT (.*?) WHERE", sparql).group(1)
        sparql = re.sub("SELECT (.*?) WHERE", "SELECT %s WHERE" % (" ".join(all_select_items)), sparql)
        return sparql

    def dfs(self, start_i, present_i, predicates_list, predicates_graph, storage_set, present_set):
        # print(present_i, present_set)
        present_set.add(predicates_list[present_i])
        tempset = set()
        for p in present_set:
            tempset.add(p)
        storage_set.add(frozenset(tempset))
        for v in predicates_graph.get(predicates_list[present_i]):
            if v in present_set or v not in predicates_graph:
                continue
            self.dfs(start_i, predicates_list.index(v), predicates_list, predicates_graph, storage_set, present_set)
        present_set.remove(predicates_list[present_i])

    def construct_storage(self):
        workloads = list()
        # subworkloads.append(list())
        sparql2sql = Sparql2sql("t0", True)

        # 各subworkload，每一个元素都是一个簇（set），其中含有构成簇的星型子查询的predicates集合（frozenset）
        predicates_in_subworkloads = list()
        # predicates_in_subworkloads.append(set())

        # 各subworkload的谓词集，用于考察聚类
        predicates_set_of_subworkload = list()
        # predicates_set_of_subworkload.append(set())

        # 各subworkload的predicates graph，用于深度优先搜索，生成连通子图
        predicates_graph_of_subworkload = list()

        # clustering...
        # number_of_subqueries = 0
        for each_sparql in self.sparql_list:  # 对于每个SPARQL
            star_dict_of_each_sparql = self.extract_predicates_from_star_query(each_sparql)
            # number_of_subqueries += len(subqueries_of_each_sparql)
            print("predicates_set_of_subworkload")
            for x in predicates_set_of_subworkload:
                print(x)
            print('星型子查询')
            for subject, predicates_of_each_subquery in star_dict_of_each_sparql.items():  # 对于SPARQL内的每个星型子查询
                print(predicates_of_each_subquery)
                # print(len(predicates_of_each_subquery), end=" ")
                # sub_sparql = self.extract_star_query_from_original(each_sparql, subject, predicates_of_each_subquery)
                belong_to_exist_cluster = True
                for i, each_predicates_set_of_subworkload in enumerate(predicates_set_of_subworkload):  # 考察每一类的谓词集
                    # 如果该星型子查询和某一类谓词集有交集，则将其归到该类，设置flag并跳出
                    if predicates_of_each_subquery.intersection(each_predicates_set_of_subworkload):
                        predicates_in_subworkloads[i].add(frozenset(predicates_of_each_subquery))
                        predicates_set_of_subworkload[i] = predicates_set_of_subworkload[i].union(
                            predicates_of_each_subquery)
                        # construct predicates graph
                        for each_p1 in predicates_of_each_subquery:
                            for each_p2 in predicates_of_each_subquery:
                                if each_p2 != each_p1:
                                    predicates_graph_of_subworkload[i].setdefault(each_p1, set()).add(each_p2)
                        # subworkloads[i].append(sparql2sql.transform(sub_sparql))
                        belong_to_exist_cluster = False
                        break
                if belong_to_exist_cluster:  # new cluster
                    predicates_in_subworkloads.append(set())
                    predicates_in_subworkloads[-1].add(frozenset(predicates_of_each_subquery))
                    predicates_set_of_subworkload.append(predicates_of_each_subquery)
                    predicates_graph_of_subworkload.append(dict())
                    # construct predicates graph
                    for each_p1 in predicates_of_each_subquery:
                        for each_p2 in predicates_of_each_subquery:
                            if each_p2 != each_p1:
                                predicates_graph_of_subworkload[-1].setdefault(each_p1, set()).add(each_p2)
                    # subworkloads.append(list())
                    # subworkloads[-1].append(sparql2sql.transform(sub_sparql))
        # print("total of subqueries:", number_of_subqueries)
        # for i in range(len(subworkloads)):
        #     print("size of subworkload {0}:".format(i + 1), len(subworkloads[i]))
        #     print("size of predicates {0}:".format(i + 1), len(predicates_set_of_subworkload[i]))

        # enumerating...
        candidate_storage = set()
        # for i, each_predicates_graph in enumerate(predicates_graph_of_subworkload):
        #     print(i)
        #     for k, v in each_predicates_graph.items():
        #         print(k, ":", v)

        for cluster_i, predicates_set in enumerate(predicates_set_of_subworkload):
            # print(cluster_i, len(predicates_set))
            predicates_list = list(predicates_set)
            predicates_list.sort()
            if len(predicates_list) == 1:
                storage_set = set()
                storage_set.add(predicates_list[0])
                candidate_storage.add(frozenset(storage_set))
            else:
                for i in range(len(predicates_list)):
                    storage_set = set()
                    self.dfs(i, i, predicates_list, predicates_graph_of_subworkload[cluster_i], storage_set, set())
                    for each_storage in storage_set:
                        candidate_storage.add(each_storage)
                        # for t in each_storage:
                        #     print(predicates_list.index(t), end=' ')
                        # print()
        print(len(candidate_storage))

        '''
        print("簇个数：", len(predicates_set_of_subworkload))
        for cluster_i, predicates_set in enumerate(predicates_set_of_subworkload):
            print("cluster {0}, 谓词个数 {1}".format(cluster_i + 1, len(predicates_set)))
            for i in range(0, len(predicates_set)):
                print("C_{0}^{1}: {2}, 总数:{3}".format(len(predicates_set), i + 1,
                                                      int(comb(len(predicates_set), i + 1)), len(candidate_storage)))
                for e in combinations(predicates_set, i + 1):
                    candidate_storage.add(frozenset(e))
        '''
        # print(len(candidate_storage))
        for each_sparql in self.sparql_list:
            workloads.append(sparql2sql.transform(each_sparql))
        return workloads, predicates_set_of_subworkload, candidate_storage


if __name__ == '__main__':
    # storage_constructor = StorageConstructor("../res/watdiv/workload_1_0_L.txt")
    storage_constructor = StorageConstructor("../res/watdiv/workload_sparql_S_x2_25.txt")
    subworkloads, predicates_set_of_subworkload, candidate_storage = storage_constructor.construct_storage()
    # for each_candidate_storage in candidate_storage:
    #     if "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in each_candidate_storage:
    #         print(each_candidate_storage)
    # for each_query in subworkloads:
    #     print(each_query)
    # k = 0
    # for i, each_subworkload in enumerate(subworkloads):
    #     print(i)
    #     for each_subquery in each_subworkload:
    #         print(each_subquery)
    #         k += 1
    # print(k)
