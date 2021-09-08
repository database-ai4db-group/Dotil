# -*- coding: utf-8 -*-
'''
This file is used to load data into PostgreSQL with psycopg2 module.
'''
import re
import psycopg2

conn = psycopg2.connect(database="yago", user="postgres", password="123456", host="127.0.0.1", port="5432")
cur = conn.cursor()
cur.execute(
    "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename;")
table_names = cur.fetchall()
# 依次删除现有表
for table_name in table_names:
    cur.execute("drop table {0};".format(table_name[0]))
source_t0_path = "../res/yago/yago150w.txt"
cur.execute("create table t0 (s text null, p text null, o text null)")
with open(source_t0_path) as f:
    cur.copy_from(f, "t0", sep="\t")
cur.execute("create index t0_s on t0(s)")
conn.commit()

# with open("/home/szm/yagoData/yagoData.csv", "r") as f:
#     lines = [x.strip() for x in f.readlines()]
# data = list()
# for line in lines:
#     matcher = re.match(r'(<.*?>).*?(<.*?>).*?(?:(".*?"|<.*?>))', line)
#     if matcher:
#         data.append(matcher.groups())
# cur.executemany("insert into t0 (s,p,o) values (%s,%s,%s)", data)
conn.commit()

cur.close()
conn.close()
