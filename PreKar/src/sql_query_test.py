# -*- coding: utf-8 -*-
# @Time  : 2021/3/9 13:07
# @Author: Jennie Qi
# @File  : sql_query_test.py
'''
This file is used to test whether all of the queries in the workload have at least one result record.
'''
import time

import psycopg2

with open("../res/yago/workload_sql_x10", "r") as f:
    lines = f.readlines()

conn = psycopg2.connect(database="yago", user="postgres", password="123456", host="127.0.0.1", port="5432")
cur = conn.cursor()
for i, line in enumerate(lines):
    start = time.time()
    cur.execute(line.strip())
    _ = cur.fetchall()
    end = time.time()
    if len(_) > 0:
        print(i + 1, "{0}条".format(len(_)), "{0}s".format(end - start))
    else:
        print("!!!!!!!!!!", i + 1, "{0}条".format(len(_)), "{0}s".format(end - start))
