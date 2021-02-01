"""
author: zhr
Foresee Mode.
"""


import os
import re
import threading
import pymysql
import numpy as np
import random
import time
from py2neo import Graph
import inspect
import ctypes
from urllib.request import urlretrieve
from py2neo.packages.httpstream import http

from find_oname import find_oname
from util import stop_thread, count_rate, transfer_neo4j_import, get_query_order

http.socket_timeout = 9999


class Foresee:
    def __init__(self, alpha_test, gamma_test, percent, prob, neo4j_times, flag, max_time, neo4j_number=0, query_order=1):
        print("connecting...")
        self.flag = flag  # 判断是哪个数据集
        self.db = pymysql.connect(host='localhost', port=3306, user='root',
                                  passwd='123456', db='query221', charset='utf8')
        self.cursor = self.db.cursor()  # 数据库连接
        self.graph = Graph('http://localhost:7474', username='neo4j', password='123456', secure=False,
                           bolt=False)  # neo4j连接
        self.gamma = gamma_test  # gamma，折旧率
        self.alpha = alpha_test  # alpha，学习率
        self.transfer_percent = percent  # neo4j / mysql threshold
        self.prob = prob  # 随机概率
        self.neo4j_times = neo4j_times  # mysql超时时长倍数
        self.max_time = max_time  # 超时时长
        self.neo4j_number = neo4j_number  # 识别neo4j并发号
        f_p = open("../doc/dbpedia_p_record.txt", 'r', encoding='utf8')  # 存储了p和p的个数文件
        lines = f_p.readlines()
        f_p.close()
        if self.flag == 0:
            lines = lines[1:40]
        elif self.flag == 3:
            lines = lines[42:60]
        elif self.flag == 1 or self.flag == 4 or self.flag == 5 or self.flag == 6:
            lines = lines[63:148]
        elif self.flag == 2:
            lines = lines[151:317]
        self.reward = dict()  # Reward矩阵    need to save
        self.q = dict()  # Q矩阵                       need to save
        self.transfer_record = dict()  # 记录p是否transfer     need to save
        self.total_neo4j_number = 0  # Neo4j total number of transfered triples     need to save
        self.query_order = query_order
        # is_loaded = False
        self.numbers = dict()  # record the number of triples of predicate
        for line in lines:
            result = re.search("([^\t]*)\t([^\t]*)", line)
            self.numbers[result.group(1)] = result.group(2)
            self.reward[result.group(1)] = np.zeros((2, 2))
            self.q[result.group(1)] = np.zeros((2, 2))
            self.transfer_record[result.group(1)] = False
        self.this_time_sql = 0.0
        # is_loaded = self.load()
        if self.flag == 0:
            self.total_number = 16418085
        elif self.flag == 1 or self.flag == 4 or self.flag == 5 or self.flag == 6:
            self.total_number = 14634621
        elif self.flag == 2:
            self.total_number = 60241165
        elif self.flag == 3:
            self.total_number = 117489648
        print("Mysql tuple total numbers:" + str(self.total_number))
        self.neo4j_query_time = 0
        self.query_time = 0
        self.mysql_query_total_time = 0.0  # RDB query总时间
        self.neo4j_query_total_time = 0.0  # GDB query总时间
        self.transfer_total_time = 0.0  # transfer总时间
        self.total_batch_time = 0
        self.breakdown = dict()  # 记录b) breakdown
        self.this_time_results = []
        self.how_much = 0
        self.this_time_neo = 0.0
        self.overtime_query = dict()

        self.batch_time_list = []
        self.rdb_batch_time = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
        self.gdb_batch_time = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
        self.transfer_batch_time = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
        self.training_batch_time = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
        self.offandon_batch_time = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}

        self.foresee_number = 0

        # self.save(self.flag)
        # exit(0)
        print("Connecting success!")

    def get_file_paths(self, flag=0):
        if self.flag == 0:
            record_path = '../doc/records/yago_record.txt'
            file_name = '../doc/mysql/yago_mysql.txt'
            file_cypher_name = '../doc/cypher/yago_cypher.txt'
            random_file_name = '../doc/random_numbers/yago_random_numbers.txt'
        elif self.flag == 1:
            record_path = '../doc/records/watdiv_L_record.txt'
            file_name = '../doc/mysql/watdiv_L_mysql.txt'
            file_cypher_name = '../doc/cypher/watdiv_L_cypher.txt'
            random_file_name = '../doc/random_numbers/watdivL_random_numbers.txt'
        elif self.flag == 2:
            record_path = '../doc/records/bio2rdf_record.txt'
            file_name = '../doc/mysql/bio2rdf_mysql.txt'
            file_cypher_name = '../doc/cypher/bio2rdf_cypher.txt'
            random_file_name = '../doc/random_numbers/bio2rdf_random_numbers.txt'
        elif self.flag == 3:
            record_path = '../doc/records/lubm_record.txt'
            file_name = '../doc/mysql/lubm_mysql.txt'
            file_cypher_name = '../doc/cypher/lubm_cypher.txt'
            random_file_name = '../doc/random_numbers/lubm_random_numbers.txt'
        elif self.flag == 4:
            record_path = '../doc/records/watdiv_S_record.txt'
            file_name = '../doc/mysql/watdiv_S_mysql.txt'
            file_cypher_name = '../doc/cypher/watdiv_S_cypher.txt'
            random_file_name = '../doc/random_numbers/watdivS_random_numbers.txt'
        elif self.flag == 5:
            record_path = '../doc/records/watdiv_F_record.txt'
            file_name = '../doc/mysql/watdiv_F_mysql.txt'
            file_cypher_name = '../doc/cypher/watdiv_F_cypher.txt'
            random_file_name = '../doc/random_numbers/watdivF_random_numbers.txt'
        else:
            record_path = '../doc/records/watdiv_C_record.txt'
            file_name = '../doc/mysql/watdiv_C_mysql.txt'
            file_cypher_name = '../doc/cypher/watdiv_C_cypher.txt'
            random_file_name = '../doc/random_numbers/watdivC_random_numbers.txt'
        if flag == 0:
            return file_name, file_cypher_name, random_file_name
        else:
            return record_path

    def load(self):
        record_path = self.get_file_paths(1)
        if not os.path.exists(record_path):
            return False
        f = open(record_path, 'r', encoding='utf8')
        total_neo4j_number = f.readline()
        self.total_neo4j_number = int(total_neo4j_number.replace('\n', ''))
        f.readline()
        record_line = f.readline()
        while record_line != '\n':
            result = re.search("([^\t]*)\t([^\t\n]*)", record_line)
            if result:
                result_string = result.group(2).replace('\n', '')
                if result_string == 'False':
                    self.transfer_record[result.group(1)] = False
                elif result_string == 'True':
                    self.transfer_record[result.group(1)] = True
                else:
                    print("load record error!")
            record_line = f.readline()
        reward_line = f.readline()
        while reward_line != '\n':
            result = re.search("([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t\n]*)", reward_line)
            if result:
                self.reward[result.group(1)][0][0] = float(result.group(2))
                self.reward[result.group(1)][0][1] = float(result.group(3))
                self.reward[result.group(1)][1][0] = float(result.group(4))
                self.reward[result.group(1)][1][1] = float(result.group(5).replace('\n', ''))
            reward_line = f.readline()
        q_line = f.readline()
        while q_line:
            result = re.search("([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)\t([^\t\n]*)", q_line)
            if result:
                self.q[result.group(1)][0][0] = float(result.group(2))
                self.q[result.group(1)][0][1] = float(result.group(3))
                self.q[result.group(1)][1][0] = float(result.group(4))
                self.q[result.group(1)][1][1] = float(result.group(5).replace('\n', ''))
            q_line = f.readline()
        return True

    def save(self):
        record_path = self.get_file_paths(1)
        f = open(record_path, 'w', encoding='utf8')
        # first record total neo4j number
        f.write(str(self.total_neo4j_number) + "\n")
        f.write('\n')  # different block interval by \n
        for p in self.transfer_record.keys():
            f.write(p + "\t" + str(self.transfer_record[p]) + '\n')
        f.write('\n')
        for p in self.reward.keys():
            f.write(p + "\t" + str(self.reward[p][0][0]) + "\t" + str(self.reward[p][0][1])
                    + "\t" + str(self.reward[p][1][0]) + "\t" + str(self.reward[p][1][1]) + "\n")
        f.write('\n')
        for p in self.q.keys():
            f.write(p + "\t" + str(self.q[p][0][0]) + "\t" + str(self.q[p][0][1])
                    + "\t" + str(self.q[p][1][0]) + "\t" + str(self.q[p][1][1]) + "\n")

    def rdb_query_time(self, query):
        """计算rdb的query时间"""
        db = pymysql.connect(host='localhost', port=3306, user='root',
                             passwd='123456', db='query221', charset='utf8')
        cursor = db.cursor()
        start = time.perf_counter()

        try:
            cursor.execute(query)
        except Exception as e:
            print("_+_+_+_+_+_+_+_+_+_+_+_+_+___+__+_+_+_+_+_+_+")
            # print(query)
            print(e)
            print("_+_+_+_+_+_+_+_+_+_+_+_+_+___+__+_+_+_+_+_+_+")
        end = time.perf_counter()
        result = cursor.fetchall()
        print("MySQL query result length:" + str(len(result)))
        db.close()
        self.this_time_sql = end - start

    def neo_query_time(self, query, length):
        graph = Graph('http://localhost:7474', username='neo4j', password='123456', secure=False,
                      bolt=False)  # neo4j连接
        start = time.perf_counter()
        results = graph.run(query)
        end = time.perf_counter()
        result_list = []
        count = 0
        for result in results:
            if len(tuple(result)) == 1 or length == 1:
                result_list.append(tuple(result)[0])
            else:

                result_list.append(tuple(result[:length]))
            count += 1
        print("Neo4j  query result:" + str(len(list(set(result_list)))) + "\t")
        if count == 0:
            print(query)
        self.this_time_results = list(set(result_list))
        self.this_time_neo = end - start

    def rdb_second_query(self, query, parameters):
        if query == "":
            print("Because of the whole query is a sub query, there is no other query")
            return
        db = pymysql.connect(host='localhost', port=3306, user='root',
                             passwd='123456', db='query221', charset='utf8')
        cursor = db.cursor()
        start = time.perf_counter()
        query += ";"
        all_result = []

        try:
            if isinstance(parameters[0], str):
                for parameter in parameters:
                    cursor.execute(query, parameter)
                    result = cursor.fetchall()
                    if len(result) != 0:
                        all_result.append(result)

            else:
                for parameter in parameters:
                    cursor.execute(query % str(parameter))
                    result = cursor.fetchall()
                    if len(result) != 0:
                        all_result.append(result)
        except Exception as e:
            print("+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")
            # print(query)
            print(parameters[:2])
            print(e)
            print("+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")
        if len(all_result) == 0:
            print("+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")
            # print(query)
            print(parameters[:2])
            print("+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")


        end = time.perf_counter()
        self.query_time = end - start
        # for result_number in range(0, len(all_result), int(len(all_result) / 10)):
        #     print(all_result[result_number])
        print("The final query result length: " + str(len(all_result)))
        db.close()

    def show_transferred_p(self):
        p_list = []
        for p in self.transfer_record.keys():
            if self.transfer_record[p]:
                p_list.append(p)
        print("transferred p: " + str(p_list))

    def foresee(self):
        self.batch_time_list = []
        file_name, file_cypher_name, random_file_name = self.get_file_paths()
        f_mysql = open(file_name, 'r', encoding='utf8')
        f_cypher = open(file_cypher_name, 'r', encoding='utf8')
        mysql_queries = f_mysql.readlines()
        cypher_queries = f_cypher.readlines()

        query_order = get_query_order(random_file_name, len(mysql_queries), self.query_order)
        wufenzhiyi = len(query_order) / 5
        total_start = time.perf_counter()
        for batch_number in range(5):
            start_batch_number = int(batch_number * wufenzhiyi)
            end_batch_number = int((batch_number + 1) * wufenzhiyi)
            if batch_number == 9:
                end_batch_number = len(mysql_queries) - 1
            batch_p_list = set()  # 存储每批次的p

            total_batch_start = time.perf_counter()
            print("+++++++++++++++++++++++++foresee+++++++++++++++++++++++")
            foresee_dict = {}
            foresee_number_dict = {}
            for p in self.transfer_record.keys():
                self.transfer_record[p] = False
            for query_number in query_order[start_batch_number:end_batch_number]:
                flag, sub_p_list, new_mysql, new_cypher, other_mysql, length = \
                    find_oname(mysql_queries[query_number], cypher_queries[query_number])
                if not flag:
                    continue
                list_hash = hash(tuple(sub_p_list))
                if list_hash not in foresee_number_dict:
                    foresee_number_dict[list_hash] = sub_p_list
                if list_hash in foresee_dict:
                    foresee_dict[list_hash] += 1
                else:
                    foresee_dict[list_hash] = 0
            print("Foresee number list:" + str(foresee_number_dict))
            print("Foresee Dict:" + str(foresee_dict))
            for key, value in foresee_dict.items():
                if value == max(foresee_dict.values()):
                    print("Foresee p list:" + str(key))
                    for p in foresee_number_dict[key]:
                        self.transfer_record[p] = True
                    transfer_batch_time = transfer_neo4j_import(set(foresee_number_dict[key]), self.flag)
                    break
            batch_start = time.perf_counter()
            for query_number in query_order[start_batch_number:end_batch_number]:
                print("===================================================")
                print("start query online:" + str(query_number))
                fail_start = time.perf_counter()
                # 首先判断子结构
                flag, sub_p_list, new_mysql, new_cypher, other_mysql, length = \
                    find_oname(mysql_queries[query_number], cypher_queries[query_number])
                print(new_mysql)
                print(new_cypher)
                print(other_mysql)
                print(length)
                # 如果没有子结构
                if not flag:
                    print("There is no sub query struct, start MySQL Query instead")
                    t = threading.Thread(target=self.rdb_query_time, args=(mysql_queries[query_number],))
                    t.start()
                    block_time = self.max_time
                    t.join(block_time)
                    if t.is_alive():
                        stop_thread(t)
                        mysql_time = block_time
                        print("Stop the mysql query immediately, time:" + str(mysql_time))
                        # self.mysql_query_total_time += mysql_time
                        fail_end = time.perf_counter()
                        self.overtime_query[query_number] = fail_end - fail_start
                        self.rdb_batch_time[batch_number] += self.max_time
                    else:
                        mysql_time = self.this_time_sql
                        print("mysql query successfully , time:" + str(mysql_time))
                        self.mysql_query_total_time += mysql_time
                        self.rdb_batch_time[batch_number] += mysql_time
                    breakdown_time = time.perf_counter()
                    self.breakdown[query_number] = breakdown_time - total_start
                    print("immediately mysql query over, start next query")
                    continue
                else:
                    print("detected the sub query struct")

                # 获取p的占比
                sub_p_rate = count_rate(sub_p_list)
                print("Get the p rates: " + str(sub_p_rate))
                for p in sub_p_list:  # 取出本次子结构没有transfer的p
                    if not self.transfer_record[p]:
                        batch_p_list.add(p)

                # 判断是否全部在Neo4j中
                transfer_flag = False
                for p in sub_p_list:
                    if not self.transfer_record[p]:  # 有一个没transfer的就为True
                        transfer_flag = True
                self.show_transferred_p()
                # 如果一个没transfer的都没有,全部已经transfer
                if not transfer_flag:  # 如果一个没transfer的都没有,全部已经transfer
                    print("All p in sub query is transferred, use Neo4j to query")
                    t2 = threading.Thread(target=self.neo_query_time, args=(new_cypher, length))
                    t2.start()
                    t2.join(100)
                    if t2.is_alive():
                        stop_thread(t2)
                        print("Neo4j over time!")
                        self.gdb_batch_time[batch_number] += 100
                        continue
                    else:
                        self.gdb_batch_time[batch_number] += self.this_time_neo
                        print("neo4j query success")

                    neo4j_time, result_list = self.this_time_neo, self.this_time_results
                    # self.update_after_transfer(new_mysql, neo4j_time, sub_p_rate)
                    if len(result_list) != 0:
                        t1 = threading.Thread(target=self.rdb_second_query, args=(other_mysql, result_list))
                        t1.start()
                        t1.join(self.max_time)
                        if t1.is_alive():
                            stop_thread(t1)
                            print("Final mysql query overtime:" + str(self.max_time))
                            fail_end = time.perf_counter()
                            self.overtime_query[query_number] = fail_end - fail_start
                            self.rdb_batch_time[batch_number] += self.max_time
                        else:
                            self.rdb_batch_time[batch_number] += self.query_time
                            print("Final query run successfully:" + str(self.query_time))
                    else:
                        print("Because of the sub query result length is 0, so jump over the other query")
                    continue
                # 如果有一个没transfer的，就在mysql中直接进行
                else:
                    print("have some p not in neo4j, choose to use mysql")
                    t = threading.Thread(target=self.rdb_query_time, args=(mysql_queries[query_number],))
                    t.start()
                    block_time = self.max_time
                    t.join(block_time)
                    print("mysql is still running:" + str(t.is_alive()))
                    if t.is_alive():
                        stop_thread(t)
                        mysql_time = block_time
                        print("force stop mysql, time:" + str(mysql_time))
                        self.mysql_query_total_time += mysql_time
                        fail_end = time.perf_counter()
                        self.overtime_query[query_number] = fail_end - fail_start
                        self.rdb_batch_time[batch_number] += self.max_time
                    else:
                        mysql_time = self.this_time_sql
                        self.rdb_batch_time[batch_number] += mysql_time
                        print("mysql query successfully, time:" + str(mysql_time))
                        self.mysql_query_total_time += mysql_time
                    breakdown_time = time.perf_counter()
                    self.breakdown[query_number] = breakdown_time - total_start
                    continue
            batch_end = time.perf_counter()
            self.total_batch_time += batch_end - batch_start
            self.batch_time_list.append(batch_end - batch_start)
            print("The No." + str(batch_number) + " batch query Online over, time :" + str(batch_end - batch_start))
            total_batch_end = time.perf_counter()
            self.offandon_batch_time[batch_number] += total_batch_end - total_batch_start
       # self.show_matrix()
        print('\n\n\n')
        total_end = time.perf_counter()
        # self.db.close()
        q_values = []
        q00 = 0
        q01 = 0
        q10 = 0
        q11 = 0
        for p in self.q.keys():
            q00 += self.q[p][0][0]
            q01 += self.q[p][0][1]
            q10 += self.q[p][1][0]
            q11 += self.q[p][1][1]
        q_values.append(q00)
        q_values.append(q01)
        q_values.append(q10)
        q_values.append(q11)

        expect_failed = self.total_batch_time
        overtime_list = []
        for number in self.overtime_query.keys():
            overtime_list.append(number)
            expect_failed -= self.overtime_query[number]
        print("over time queries:" + str(overtime_list))
        print("total time after strip:" + str(expect_failed))
        print(self.batch_time_list)
        self.save()
        print("RDB time: " + str(self.rdb_batch_time))
        print("GDB time: " + str(self.gdb_batch_time))
        print("Transfer time: " + str(self.transfer_batch_time))
        print("Training time: " + str(self.training_batch_time))
        print("Offline + Online Batch time: " + str(self.offandon_batch_time))

        self.rdb_batch_time = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
        self.gdb_batch_time = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
        self.transfer_batch_time = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
        self.training_batch_time = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
        self.offandon_batch_time = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}

        return total_end - total_start, q_values, self.total_batch_time, overtime_list, expect_failed, self.batch_time_list

    def show_matrix(self):
        print("\n\n\nshow transfer record")
        print(str(self.transfer_record))

        print("show Reward Matrix")
        for p in self.reward.keys():
            print("p:" + str(p) + "\n" + str(self.reward[p]) + '\n')

        print("show Q Matrix")
        for p in self.q.keys():
            print("p:" + str(p) + "\n" + str(self.q[p]) + '\n')

        # print("neo4j average:" + str(self.neo4j_query_time / self.how_much))
