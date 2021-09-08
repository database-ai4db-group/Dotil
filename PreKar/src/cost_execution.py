# -*- coding: utf-8 -*-
'''
This file is used to generate and collect train data.
'''
import shelve
import sys
import os
from datetime import datetime

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

from src.sql_rewriter import SqlRewriter
from src.construct_storage import StorageConstructor
import psycopg2
import time


def construct_join_sql(sql, temp_storage_list, i, all_predicates_types, all_types):
    """
    递归构造，获取存储结构数据的sql语句
    :param sql: 当前（递归层）已有的sql语句
    :param temp_storage_list:   存储结构
    :param i:   当前（递归层）序号，最大值为存储结构的大小，初值为1
    :param all_predicates_types:    列名LIST（所有谓词和所有type的宾语集LIST）
    :param all_types:   所有type的宾语集LIST
    :return:    获取存储结构temp_storage_list数据的sql语句
    """
    # 递归结束条件
    if i > len(temp_storage_list):
        return sql
    # 递归开始条件（i初值必须为1）
    if i == 1:
        # 与当存储结构中只有一个predicate时的sql等价
        if temp_storage_list[0] not in all_types:
            return construct_join_sql(
                "select t0.s, t0.o from t0 where p = '{0}'".format(temp_storage_list[0]),
                temp_storage_list, i + 1, all_predicates_types, all_types)
        else:
            return construct_join_sql(
                "select t0.s, t0.o from t0 where p = '{0}' and o = '{1}'".format(
                    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", temp_storage_list[0]),
                temp_storage_list, i + 1, all_predicates_types, all_types)
    if i == 2:
        # 与当存储结构中只有两个predicate时的sql等价
        if temp_storage_list[i - 1] not in all_types:
            q = "select t0.s, t0.o from t0 where p = '{0}'".format(temp_storage_list[i - 1])
        else:
            q = "select t0.s, t0.o from t0 where p = '{0}' and o = '{1}'".format(
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", temp_storage_list[i - 1])
        new_sql = "select a.s, a.{0}, a.{1} from (" \
                  "(select a.s s, a.o {0}, b.o {1} from ({2}) a left join ({3}) b on a.s=b.s)" \
                  " union " \
                  "(select b.s s, a.o {0}, b.o {1} from ({2}) a right join ({3}) b on a.s=b.s)" \
                  ") a".format(
            "pred" + str(all_predicates_types.index(temp_storage_list[i - 2])),
            "pred" + str(all_predicates_types.index(temp_storage_list[i - 1])),
            sql, q)
        return construct_join_sql(new_sql, temp_storage_list, i + 1, all_predicates_types, all_types)
    if i > 2:
        if temp_storage_list[i - 1] not in all_types:
            q = "select t0.s, t0.o from t0 where p = '{0}'".format(temp_storage_list[i - 1])
        else:
            q = "select t0.s, t0.o from t0 where p = '{0}' and o = '{1}'".format(
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", temp_storage_list[i - 1])
        new_sql = "select a.s, " + ", ".join(
            ["a.pred" + str(all_predicates_types.index(temp_storage_list[k])) for k in range(i)]) \
                  + " from (" \
                    "(select a.s, " + ", ".join(
            ["a.pred" + str(all_predicates_types.index(temp_storage_list[k])) for k in range(i - 1)]) \
                  + ", b.o pred" + str(all_predicates_types.index(temp_storage_list[i - 1])) \
                  + " from ({0}) a left join ({1}) b on a.s=b.s)".format(sql, q) \
                  + " union " \
                    "(select b.s, " + ", ".join(
            ["a.pred" + str(all_predicates_types.index(temp_storage_list[k])) for k in range(i - 1)]) \
                  + ", b.o pred" + str(all_predicates_types.index(temp_storage_list[i - 1])) \
                  + " from ({0}) a right join ({1}) b on a.s=b.s)".format(sql, q) \
                  + ") a"
        return construct_join_sql(new_sql, temp_storage_list, i + 1, all_predicates_types, all_types)


class CostExecLogger:
    '''
        日志管理器
    '''

    def __init__(self):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        self.f = open("../res/log/{0}-cost_execution.log".format(timestamp), "w", encoding='utf8')

    def write(self, s, flag=True):
        '''
        写入日志
        :param s:
        :param flag: True，打印到控制台；否则为False
        :return:
        '''
        curr_time = datetime.now()
        s_ = curr_time.strftime("%Y-%m-%d %H:%M:%S") + "\t" + str(s)
        self.f.write(s_ + "\n")
        if flag:
            print(s_)
        self.f.flush()

    def close(self):
        self.f.close()


def reinit():
    os.system("systemctl stop postgresql-13")  # 重启DB
    os.system("echo 3 > /proc/sys/vm/drop_caches")
    os.system("systemctl start postgresql-13")


if __name__ == '__main__':
    logger = CostExecLogger()
    workload_filepath = "../res/watdiv/workload_sparql_S_x1_25.txt"
    train_data_shelve_path = "../res/shelves/watdiv/train_data_S_x1_25"
    candiate_storage_list_path = "../res/shelves/watdiv/candidate_storage_list_S_x1_25"
    # 数据文件路径
    source_t0_path = "../res/watdiv/watdiv25_.txt"

    database = "watdiv"
    # 连接pg
    conn = psycopg2.connect(database=database, user="postgres", password="123456", host="localhost", port="5432")
    cur = conn.cursor()
    logger.write(workload_filepath)
    logger.write(train_data_shelve_path)
    logger.write(candiate_storage_list_path)
    logger.write("连接数据库{0}成功".format(database))
    # 获取现有表名
    cur.execute(
        "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename;")
    table_names = cur.fetchall()
    # 依次删除现有表
    for table_name in table_names:
        cur.execute("drop table {0};".format(table_name[0]))
    logger.write("已删除所有表")

    # 创建表t0
    cur.execute("create table t0 (s text null, p text null, o text null)")
    logger.write("表 t0 已创建")
    # 将数据文件导入到t0中
    with open(source_t0_path) as f:
        cur.copy_from(f, "t0", sep=" ")
    logger.write("文件{0}数据导入 t0 完毕".format(source_t0_path))
    # 在t0上建立索引
    cur.execute("create index t0_s on t0(s)")
    logger.write("t0表的s列建立索引完毕")
    # 事务更新
    conn.commit()
    logger.write("事务更新")

    # 获取所有的predicates
    cur.execute("select distinct p from t0 order by p")
    all_predicates_types = [x[0] for x in cur.fetchall()]
    # 获取types的所有objects
    cur.execute("select distinct o from t0 where p like '%type%' order by o")
    all_types = [x[0] for x in cur.fetchall()]
    # 将二者合为一个LIST，用index作为编码
    all_predicates_types.extend(all_types)
    logger.write("构造全部谓词和类型LIST完毕: all_predicates_types")

    # 初始化存储枚举器，并枚举所有候选存储结构
    storage_constructor = StorageConstructor(workload_filepath)
    workloads, predicates_set_of_subworkload, candidate_storage = storage_constructor.construct_storage()
    logger.write("存储结构枚举完成，共 {0} 种".format(len(candidate_storage)))

    # 初始化SQL改写器
    sql_rewriter = SqlRewriter(all_predicates_types)
    logger.write("SQL改写器初始化完成")
    # k = 0
    # 开始生成训练数据
    logger.write("开始生成训练数据......")
    # 将存储结构转化为LIST并排序，再保存成shelve文件
    if os.path.exists(candiate_storage_list_path + ".dat"):
        with shelve.open(candiate_storage_list_path) as f:
            candidate_storage_list = f["candidate_storage_list"]
    else:
        candidate_storage_list = list(candidate_storage)
        candidate_storage_list.sort(key=lambda storage: len(storage))
        with shelve.open(candiate_storage_list_path) as f:
            f["candidate_storage_list"] = candidate_storage_list
    '''
    # 对不同大小的存储结构进行测试，已关闭
    test_list = list()
    for i in range(1, 5):
        for c in candidate_storage_list:
            if len(c) == i:
                test_list.append(c)
                break
    '''
    # 断点续训
    if os.path.exists(train_data_shelve_path + ".dat"):
        with shelve.open(train_data_shelve_path) as f:
            train_data = f["data"]
            start_round = f["round"]
            workloads = f["workloads"]
    else:
        train_data = dict()
        start_round = -1

    train_data_shelve = shelve.open(train_data_shelve_path)
    train_data_shelve["workloads"] = workloads  # 记录下所有sql负载

    test_results_dict = dict()  # TODO 检验结果正确性

    # 获取空存储结构下的训练数据
    if start_round == -1:
        train_data_shelve["round"] = -1
        # 执行查询计划，得到训练数据
        # 构造一个DICT变量，作为SQL改写器的参数
        tables = dict()  # 空存储结构
        logger.write("执行[空存储结构]的查询计划")
        train_data_on_raw_storage = dict()  # 当前存储结构的训练数据
        for j, each_workload in enumerate(workloads):
            # timer = 0  # 子负载执行总用时
            logger.write("开始第 %d 个sub-workload的query cost execution" % (j + 1))
            # 生成改写后的sql
            execute_sql = sql_rewriter.execute(tables, each_workload)
            logger.write("执行SQL:" + execute_sql)

            start = time.time()  # 计时器（执行时）
            cur.execute(execute_sql)  # 执行sql
            _ = cur.fetchall()
            end = time.time()

            # if each_workload not in test_results_dict:
            #     test_results_dict[each_workload] = {x for x in _}
            # else:
            #     this_results_set = {x for x in _}
            #     if not (this_results_set.issubset(test_results_dict[each_workload])
            #             and test_results_dict[each_workload].issubset(this_results_set)):
            #         logger.write("结果不匹配，上次条目数{0}，本次条目数{1}".format(len(test_results_dict[each_workload]),
            #                                                       len(this_results_set)))
            #         train_data_shelve.close()
            #         sys.exit(1)

            # timer += end - start
            logger.write("执行时间: %.6f sec  结果条数: %d" % (end - start, len(_)))
            # 记录执行时间
            # train_data_on_raw_storage[each_workload] = end - start    # todo 为了保证相同的query不去重，将训练数据文件的键设为时间，值设为query
            train_data_on_raw_storage[end - start] = each_workload
        # 记录当前存储结构的训练数据
        train_data[frozenset()] = train_data_on_raw_storage
        train_data_shelve["data"] = train_data
    train_data_shelve.close()  # 关闭并刷新资源

    # start_round = 20
    cur.close()
    conn.close()

    i = start_round + 1
    while i < len(candidate_storage_list):
        # for i in range(start_round + 1, len(candidate_storage_list)):
        reinit()

        conn = psycopg2.connect(database=database, user="postgres", password="123456", host="localhost", port="5432")
        cur = conn.cursor()

        train_data_shelve = shelve.open(train_data_shelve_path)  # 重启文件资源

        each_candidate_storage = candidate_storage_list[i]
        # 将存储结构从SET转为LIST
        temp_storage_list = list(each_candidate_storage)
        logger.write(
            "第 {0} 个存储结构: ".format(i + 1) + str(each_candidate_storage) + " 共 {0} 种".format(len(candidate_storage)))

        # 删除已有表，除t0
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename;")
        table_names = cur.fetchall()
        for table_name in table_names:
            if table_name[0] != "t0":
                cur.execute("drop table {0};".format(table_name[0]))
        logger.write("删除已有表")

        conn.commit()
        logger.write("事务更新")

        # 由于逻辑比较复杂，调用递归函数处理。
        tnum = 1  # 表序号为1，实际上只需要一个额外表，tnum自增是不必要的
        start = time.time()  # 计时器（构造时）
        # 调用递归函数，生成获取存储结构数据的sql
        # 获取存储结构数据
        sql = construct_join_sql("", temp_storage_list, 1, all_predicates_types, all_types)
        logger.write("执行SQL: {0}".format(sql))
        cur.execute(sql)
        results = cur.fetchall()
        logger.write("存储结构数据获取完毕，条目:{0}".format(str(len(results))))

        # 新建表t1，并将存储结构数据插入t1，并在s列上建B-tree索引
        cur.execute(
            "create table if not exists t1 (s text null, {0})".format(
                ", ".join(
                    ["pred" + str(all_predicates_types.index(temp_storage_list[x])) + " text null"
                     for x in range(len(temp_storage_list))]
                )
            )
        )
        logger.write("表t0重新导入")

        logger.write("创建表 t{0} 完成".format(tnum))
        cur.executemany("insert into t1(s, {0}) values({1})".format(
            ", ".join(["pred" + str(all_predicates_types.index(temp_storage_list[i])) for i in
                       range(len(temp_storage_list))]),
            ", ".join(["%s" for x in range(len(temp_storage_list) + 1)])
        ), results)
        logger.write("表数据插入完成")
        cur.execute("create index t{0}_s on t{0}(s)".format(str(tnum)))
        logger.write("表t{0}的列s上建立索引完成".format(str(tnum)))

        # 从t0中删除冗余数据，并记录下来
        # 根据存储结构中的谓词是否是type相关的，分为两种情况
        delete_results = list()
        for ii in range(len(temp_storage_list)):
            if temp_storage_list[ii] not in all_types:
                cur.execute(
                    "select s, p, o from t0 where (s, o) in (select s, {0} from t1 where s is not null and {0} is not null) and p = '{1}'".format(
                        "pred" + str(all_predicates_types.index(temp_storage_list[ii])), temp_storage_list[ii]))
                delete_results.extend(list(cur.fetchall()))
                cur.execute(
                    "delete from t0 where (s, o) in (select s, {0} from t1 where s is not null and {0} is not null) and p = '{1}'".format(
                        "pred" + str(all_predicates_types.index(temp_storage_list[ii])), temp_storage_list[ii]))
            else:
                cur.execute(
                    "select s, p, o from t0 where p = '{0}' and o = '{1}'".format(
                        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", temp_storage_list[ii]))
                delete_results.extend(list(cur.fetchall()))
                cur.execute(
                    "delete from t0 where p = '{0}' and o = '{1}'".format(
                        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", temp_storage_list[ii]))
        logger.write("表t0冗余数据删除完成")
        conn.commit()
        tnum += 1
        logger.write("事务更新")
        end = time.time()
        logger.write("用时: %.6f sec" % (end - start))

        # 执行查询计划，得到训练数据
        # 构造一个DICT变量，作为SQL改写器的参数
        tables = dict()
        tables["t1"] = each_candidate_storage
        logger.write("执行查询计划")
        print(each_candidate_storage)
        train_data_on_storage = dict()  # 当前存储结构的训练数据
        rerun = False
        for j, each_workload in enumerate(workloads):
            # 定期重启清缓存
            # if (j+1) % 5 == 0:
            #     cur.close()
            #     conn.close()
            #     reinit()
            #     conn = psycopg2.connect(database=database, user="postgres", password="123456", host="localhost",
            #                             port="5432")
            #     cur = conn.cursor()
            # timer = 0  # 子负载执行总用时
            logger.write("开始第 %d 个sub-workload的query cost execution" % (j + 1))
            # 生成改写后的sql
            execute_sql = sql_rewriter.execute(tables, each_workload)
            # logger.write("执行SQL:" + execute_sql)

            start = time.time()  # 计时器（执行时）
            cur.execute(execute_sql)  # 执行sql
            _ = cur.fetchall()
            end = time.time()

            # if each_workload not in test_results_dict:
            #     test_results_dict[each_workload] = {x for x in _}
            # else:
            # this_results_set = {x for x in _}
            # # if not (this_results_set.issubset(test_results_dict[each_workload])
            # #         and test_results_dict[each_workload].issubset(this_results_set)):
            #     logger.write("结果不匹配，上次条目数{0}，本次条目数{1}".format(len(test_results_dict[each_workload]),
            #                                                   len(this_results_set)))
            # if len(this_results_set) != len(test_results_dict[each_workload]):
            #     logger.write("结果不匹配，上次条目数{0}，本次条目数{1}".format(len(test_results_dict[each_workload]),
            #                                                   len(this_results_set)))
            #     train_data_shelve.close()
            #     sys.exit(1)

            # timer += end - start
            logger.write("执行时间: %.6f sec  结果条数: %d" % (end - start, len(_)))
            if end - start < 0:
                rerun = True
                logger.write("超过50秒。")
                break
            else:
                # 记录执行时间
                # train_data_on_storage[each_workload] = end - start
                train_data_on_storage[end - start] = each_workload
        if not rerun:
            # 记录当前存储结构的训练数据
            train_data[each_candidate_storage] = train_data_on_storage

            train_data_shelve["data"] = train_data
            train_data_shelve["round"] = i
            train_data_shelve.close()  # 关闭并刷新资源
            logger.write("第 {0} 轮及之前的训练数据已保存".format(i + 1))

            i += 1

        # 恢复t0数据
        cur.executemany("insert into t0 (s, p, o) values (%s, %s, %s)", delete_results)
        conn.commit()
        logger.write("t0数据恢复完成，恢复条数:{0}".format(str(len(delete_results))))
        cur.close()
        conn.close()
