import random
import re
import sys
import threading
import time

import pymysql
import configparser
from util import *


class RDBTime:
    def __init__(self):
        self.cf = configparser.ConfigParser()
        self.cf.read("config.ini", encoding="utf-8")
        self.query_time = 0.0
        self.total_time = 0.0
        self.stack_time = dict()
        self.query_number = 100
        self.flag = int(self.cf.get("total", "db_flag"))
        self.host = self.cf.get("total", "host")
        self.port = int(self.cf.get("total", "port"))
        self.user = self.cf.get("total", "user")
        self.password = self.cf.get("total", "password")
        self.db = self.cf.get("total", "db")
        self.charset = self.cf.get("total", "charset")

    def rdb_time(self):
        file_name = self.cf.get("total", "sql_query_dir") + self.cf.get("total", "dataset") + "_sql_query.txt"
        random_file_name = self.cf.get("total", "random_query_dir") + self.cf.get("total", "dataset") + "_random.txt"
        batch_number = 1
        batch_start = time.perf_counter()
        with open(file_name, 'r', encoding='utf8') as f:
            lines = f.readlines()
            try:
                shuffle_numbers = []
                with open(random_file_name, 'r', encoding='utf8') as random_f:
                    random_lines = random_f.readlines()
                    for line in random_lines:
                        shuffle_numbers.append(int(line.rstrip('\r\n')))
            except FileNotFoundError:
                shuffle_numbers = list(range(0, len(lines)))
                random.shuffle(shuffle_numbers)
                with open(random_file_name, 'w', encoding='utf8') as f:
                    for number in shuffle_numbers:
                        f.write(str(number) + "\r\n")
            if int(self.cf.get("rdb", "random")) == 0:
                shuffle_numbers = list(range(len(lines)))

            batch_length = len(lines) / 5
            print("one in fifth:" + str(batch_length))
            batch_count = 0
            total_start = time.perf_counter()
            for line_number in shuffle_numbers:
                print("====================================================")
                print("line number:" + str(line_number))
                print("batch number:" + str(batch_number))
                t = threading.Thread(target=self.rdb_query, args=(lines[line_number].rstrip('\n'),))
                t.start()
                t.join(500)
                if t.is_alive():
                    # stop_thread(t)
                    print("overtime")
                    print("The No." + str(line_number + 1) + " query" + str(line_number) + "overtime:500s")
                else:
                    print("The No." + str(line_number + 1) + " query" + str(line_number) + "execute successfully:" + str(self.query_time))
                if (batch_count+1) % batch_length == 0 and batch_count != 0:
                    batch_end = time.perf_counter()
                    print("The No." + str(batch_number) + " batch query over, time:" + str(batch_end - batch_start))
                    batch_number += 1
                    batch_start = time.perf_counter()
                batch_count += 1
            total_end = time.perf_counter()
            print("total time :" + str(total_end - total_start))

    def rdb_query(self, query):
        db = pymysql.connect(host=self.host, port=int(self.port), user=self.user,
                             passwd=self.password, db=self.db, charset=self.charset)
        cursor = db.cursor()
        start = time.perf_counter()
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            print("MySQL query result length:" + str(len(result)))
        except Exception as e:
            print("_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")
            print(query)
            print(e)
            print("_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")
        end = time.perf_counter()
        self.query_time = end - start
        db.close()


if __name__ == '__main__':
    q = RDBTime()
    q.rdb_time()
