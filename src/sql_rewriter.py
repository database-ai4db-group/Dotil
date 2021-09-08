# -*- coding: utf-8 -*-
'''
This file is used to rewrite the SQL which is tranformed from SPARQL according to the current storage.
'''
import re


class SqlRewriter:
    '''
    SQL改写器（根据现有存储结构对raw sql进行改写）
    '''

    def __init__(self, all_predicates_types):
        self.tables = None
        self.all_predicates_types = all_predicates_types

    def extract_sql_feature(self, sql):
        '''
        解析sql中的特征
        :param sql:
        :return:
            sql_feature: 特征LIST
            predicates: 谓词集合
            predicate_dict: 字母到谓词的映射
            p_to_name_dict: 谓词到字母的映射
        '''
        where_match = re.search("where(.*)", sql)  # where子句
        conditions = [x.strip() for x in where_match.group(1).split("and")]  # 连接条件LIST

        sql_feature = list()
        predicate_dict = dict()  # char -> predicate
        predicates = set()  # predicates SET
        p_to_name_dict = dict()  # predicate -> char
        # 解析predicates条件
        for cond in conditions:
            p_match = re.search("(\\w)\\.p\\s*=\\s*'(.*)'", cond)
            if p_match:
                predicate_dict[p_match.group(1)] = p_match.group(2)
                p_to_name_dict.setdefault(p_match.group(2), list()).append(p_match.group(1))
                sql_feature.append(p_match.group(2))
                predicates.add(p_match.group(2))
        # 解析s和o上的连接条件
        for cond in conditions:
            join_match = re.search("(\\w)\\.(s|o)\\s*=\\s*(\\w)\\.(s|o)", cond)
            if join_match:
                sql_feature.append("{0}.{1}={2}.{3}".format(predicate_dict[join_match.group(1)], join_match.group(2),
                                                            predicate_dict[join_match.group(3)], join_match.group(4)))
                sql_feature.append("{2}.{3}={0}.{1}".format(predicate_dict[join_match.group(1)], join_match.group(2),
                                                            predicate_dict[join_match.group(3)], join_match.group(4)))
        if "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" in predicates:
            predicates.remove("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")
            for table_chr in p_to_name_dict.get("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"):
                obj_search = re.search("{0}.o\\s*=\\s*'(.*?)'".format(table_chr), sql)
                if obj_search:
                    p_to_name_dict.setdefault(obj_search.group(1), list()).append(table_chr)
            del p_to_name_dict["<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"]
        for k in p_to_name_dict.keys():
            if k not in predicates:
                predicates.add(k)
        return sql_feature, predicates, predicate_dict, p_to_name_dict

    def match(self, predicates):
        '''
        通过将存储结构中各表的谓词信息和predicates相比较，得到一个谓词到表名的替换映射
        :param predicates:  sql中涉及的所有谓词
        :return:    candidate_tables, 一个映射，原sql中的谓词->新表名
        '''
        candidate_tables = dict()
        for name, preds in self.tables.items():
            for p in set(preds).intersection(predicates):
                candidate_tables[p] = name
        return candidate_tables

    def rewrite(self, sql, candidate_tables, p_to_name_dict):
        '''

        :param sql:
        :param candidate_tables:
        :param p_to_name_dict:
        :return:
        '''
        # print("raw:\t", sql)
        # 遍历所有（要替换的谓词，替换的新表）
        base_i = 0
        for p, name in candidate_tables.items():
            for i, each_name in enumerate(p_to_name_dict[p]):
                new_table_name = "n" + str(base_i) + str(i)
                # "原表.s" -> "新表.s"
                sql = re.sub(" {0}\\.s".format(each_name), " {0}.s ".format(new_table_name), sql)
                # "原表.o" -> "新表.predi"
                sql = re.sub(" {0}\\.o".format(each_name),
                             " {0}.{1}".format(new_table_name, "pred" + str(self.all_predicates_types.index(p))), sql)
                # "t0 原表" -> "新表"
                sql = re.sub("t0 {0}".format(each_name), "{0} {1}".format(name, new_table_name), sql)
                # 谓词常量条件，删除
                sql1 = re.sub("{0}\\.p\\s*=\\s*'.*?'\\s*and".format(each_name), "", sql)
                if sql1 != sql:  # 如删除成功，加上"新表.predi is not null"
                    sql1 += " and {0}.{1} is not null".format(new_table_name,
                                                              "pred" + str(self.all_predicates_types.index(p)))
                    sql = sql1
                # 如未删除成功，考虑另一种情况
                sql2 = re.sub("and\\s*{0}\\.p\\s*=\\s*'.*?'".format(each_name), "", sql)
                if sql2 != sql:
                    sql2 += " and {0}.{1} is not null".format(new_table_name,
                                                              "pred" + str(self.all_predicates_types.index(p)))
                    sql = sql2
                sql3 = re.sub("where\\s*{0}\\.p\\s*=\\s*'.*?'".format(each_name), "", sql)
                if sql3 != sql:
                    sql3 += " where {0}.{1} is not null".format(new_table_name,
                                                                "pred" + str(self.all_predicates_types.index(p)))
                    sql = sql3
            base_i += 1
        # 格式化from子句，分隔符为", "
        from_match = re.search("from(.*?)where", sql)
        if from_match:
            from_clause = ", ".join([x for x in {y.strip() for y in from_match.group(1).split(",")}])
        else:
            from_match = re.search("from(.*)", sql)
            if from_match:
                from_clause = ", ".join([x for x in {y.strip() for y in from_match.group(1).split(",")}])
        if not re.search("from(.*?)where", sql):
            sql = re.sub("from(.*)", "from {0}".format(from_clause), sql)
        else:
            sql = re.sub("from(.*)where", "from {0} where".format(from_clause), sql)
        # 删除可能的星型连接条件
        # for p, name in candidate_tables.items():
        #     sql = re.sub("{0}\\.s\\s*=\\s*{0}\\.s\\s*and".format(name), "", sql)
        #     sql = re.sub("and\\s*{0}\\.s\\s*=\\s*{0}\\.s".format(name), "", sql)
        # 加上distinct判断
        sql = sql.replace("select", "select distinct")
        return sql

    def execute(self, tables, sql):
        '''
        执行改写
        :param sql: 要改写的sql
        :return: 改写后的sql
        '''
        self.tables = tables
        feature, predicates, predicate_dict, p_to_name_dict = self.extract_sql_feature(sql)
        # print(predicates)
        # print(p_to_name_dict)
        candidate_tables = self.match(predicates)
        # print(candidate_tables)
        return self.rewrite(sql, candidate_tables, p_to_name_dict)


if __name__ == '__main__':
    # with open("../res/watdiv/sql/c1", "r") as f:
    #     sqls = [x.strip() for x in f.readlines()]
    # tables = {
    #     't1': ('http://db.uwaterloo.ca/~galuc/wsdbm/subscribes', 'http://db.uwaterloo.ca/~galuc/wsdbm/likes'),
    #     't2': ('http://schema.org/nationality', 'http://schema.org/jobTitle'),
    #     't3': ('http://www.geonames.org/ontology#parentCountry',),
    #     't4': ('http://ogp.me/ns#tag', 'http://schema.org/caption'),
    # }
    sql_rewriter = SqlRewriter("t0")
    gg = set()
    gg.add('<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse>')
    tables = dict()
    tables["t1"] = frozenset(gg)
    print(tables)
    sql = "select a.s, b.s from t0 d, t0 c, t0 b, t0 a where a.s = c.s and b.s = c.o and c.o = d.o and a.p = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' and a.o = '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>' and b.p = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' and b.o = '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateCourse>' and c.p = '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse>' and d.s = '<http://www.Department10.University3.edu/AssociateProfessor10>' and d.p = '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf>'"
    print(sql_rewriter.execute(tables, sql))
