# -*- coding: utf-8 -*-
'''
This file is used to transfer SPARQL to SQL.
'''
import os
import re
import queue


class Entity:
    def __init__(self, s=None, p=None, o=None, t=None):
        self.s = s
        self.p = p
        self.o = o
        self.t = t

    def __str__(self):
        return "%s: (%s, %s, %s)" % (self.t, self.s, self.p, self.o)

    def __repr__(self):
        return "%s: (%s, %s, %s)" % (self.t, self.s, self.p, self.o)


class Sparql2sql:
    '''
    SPARQL to SQL transformer.
    '''

    def __init__(self, table_name, p_angle_brackets=True):
        self.JOIN_TO_TYPE = {
            ("s", "s"): 1,
            ("s", "o"): 2,
            ("o", "s"): 3,
            ("o", "o"): 4
        }
        self.TYPE_TO_JOIN = ["OCCUPIED", ("s", "s"), ("s", "o"), ("o", "s"), ("o", "o")]
        self.table_name = table_name
        self.p_angle_brackets = p_angle_brackets

    def transform(self, sparql):
        select_conditions_list = self.__parse_select(sparql)
        entities = self.__parse_where(sparql)
        queue_map = self.__construct_queue_map(entities)
        join_conditions = self.__construct_join_conditions(entities, queue_map)
        from_sql = self.__construct_from_sql(join_conditions)
        if not from_sql.strip():
            from_sql = ", ".join(["{0} {1}".format(self.table_name, entity.t) for entity in entities if
                                  entity.s.startswith("?") or entity.p.startswith("?") or entity.o.startswith("?")])
        where_sql, select_map = self.__construct_where_sql(join_conditions, entities)
        select_sql = self.__construct_select_sql(select_conditions_list, select_map)
        # sql = "select %s from %s where %s" % (select_sql, from_sql, where_sql)
        sql = "select * from %s where %s" % (from_sql, where_sql)
        return sql

    def __parse_select(self, sparql):
        '''
        解析 select，返回一个 list
        :param sparql:
        :return: e.g. ['X', 'Y']
        '''
        select_match = re.search("SELECT(.*)WHERE", sparql, re.S)
        if select_match:
            select_clause = select_match.group(1).strip()
        else:
            select_clause = None
        select_conditions = select_clause.split()    # todo LUBM数据集中select items使用","分割
        for i in range(len(select_conditions)):
            select_conditions[i] = select_conditions[i].replace("?", "").strip()
        return select_conditions

    def __parse_where(self, sparql):
        '''
        解析 where，构造实体列表，返回一个 list
        :param sparql:
        :return: a list of Entities. e.g. a: <?X, http://www.w3.org/1999/02/22-rdf-syntax-ns#type, http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Chair>
        '''
        where_match = re.search("WHERE\\s*\{(.*)\}", sparql, re.S)
        if where_match:
            where_clause = where_match.group(1)
        else:
            where_clause = None
        wheres = [x.strip().split() for x in where_clause.split(".\n") if x.strip()]
        t = ord('a')
        entities = list()
        for where in wheres:
            if not self.p_angle_brackets:
                for i in range(len(where)):
                    where[i] = where[i].replace("<", "").replace(">", "")
            entities.append(Entity(where[0], where[1], where[2], chr(t)))
            t += 1
        return entities

    def __construct_queue_map(self, entities):
        '''
        根据实体列表，初始化队列映射，返回一个 map
        :param entities:
        :return: used for constructing sql about join. e.g. {'X': queue.Queue(), 'Y': queue.Queue()}
        '''
        queue_map = {}
        for i in range(len(entities)):
            if entities[i].s.startswith("?"):
                c = entities[i].s[1:]
                if c not in queue_map:
                    queue_map[c] = queue.Queue()
            if entities[i].p.startswith("?"):
                c = entities[i].p[1:]
                if c not in queue_map:
                    queue_map[c] = queue.Queue()
            if entities[i].o.startswith("?"):
                c = entities[i].o[1:]
                if c not in queue_map:
                    queue_map[c] = queue.Queue()
        return queue_map

    def __construct_join_conditions(self, entities, queue_map):
        '''
        根据实体列表和初始化的队列映射，构造连接条件列表，其中每一个元素都是一个三元组，返回一个 list
        :param entities:
        :param queue_map:
        :return: e.g. [('a', 'c', 1), ('b', 'c', 2), ('c', 'd', 3)]
        '''
        join_conditions = []
        for i in range(len(entities)):
            if entities[i].s.startswith("?"):
                c = entities[i].s[1:]
                if not queue_map[c].empty():
                    last_tuple = queue_map[c].get()
                    join_conditions.append((last_tuple[0], entities[i].t, self.JOIN_TO_TYPE[(last_tuple[1], "s")]))
                queue_map[c].put((entities[i].t, "s"))
            if entities[i].p.startswith("?"):
                c = entities[i].p[1:]
                if not queue_map[c].empty():
                    last_tuple = queue_map[c].get()
                    join_conditions.append((last_tuple[0], entities[i].t, self.JOIN_TO_TYPE[(last_tuple[1], "p")]))
                queue_map[c].put((entities[i].t, "p"))
            if entities[i].o.startswith("?"):
                c = entities[i].o[1:]
                if not queue_map[c].empty():
                    last_tuple = queue_map[c].get()
                    join_conditions.append((last_tuple[0], entities[i].t, self.JOIN_TO_TYPE[(last_tuple[1], "o")]))
                queue_map[c].put((entities[i].t, "o"))
        return join_conditions

    def __construct_from_sql(self, join_conditions):
        '''
        根据连接条件列表，构造 sql 中的 from 子句
        :param join_conditions:
        :return: e.g. t0 c, t0 b, t0 d, t0 a
        '''
        table_set = set()
        for join_condition in join_conditions:
            table_set.add(join_condition[0])
            table_set.add(join_condition[1])
        from_sql = ", ".join(["{0} {1}".format(self.table_name, x) for x in table_set])
        return from_sql

    def __construct_where_sql(self, join_conditions, entities):
        '''
        根据连接条件列表和实体列表，构造 sql 中的 where 子句，同时构造一个 map，用于后面 select 子句的构造
        :param join_conditions:
        :param entities:
        :returns where_sql: e.g.  a.s = c.s and b.s = c.o and c.o = d.s and a.p = http://www.w3.org/1999/02/22-rdf-syntax-ns#type and a.o =...[truncated]
        :returns select_map: Besides, it returns a map. e.g. {'X': a, 'Y': b}
        '''
        select_map = {}
        where_sql = " and ".join(["%s.%s = %s.%s" % (
            join_condition[0], self.TYPE_TO_JOIN[join_condition[2]][0], join_condition[1],
            self.TYPE_TO_JOIN[join_condition[2]][1]) for join_condition in join_conditions])
        for entity in entities:
            if not entity.s.startswith("?"):
                where_sql += " and %s.s = '%s'" % (entity.t, entity.s)
            else:
                c = entity.s.replace("?", "")
                if c not in select_map:
                    select_map[c] = "%s.%s" % (entity.t, "s")
            if not entity.p.startswith("?"):
                where_sql += " and %s.p = '%s'" % (entity.t, entity.p)
            else:
                c = entity.p.replace("?", "")
                if c not in select_map:
                    select_map[c] = "%s.%s" % (entity.t, "p")
            if not entity.o.startswith("?"):
                where_sql += " and %s.o = '%s'" % (entity.t, entity.o)
            else:
                c = entity.o.replace("?", "")
                if c not in select_map:
                    select_map[c] = "%s.%s" % (entity.t, "o")
        where_sql = where_sql.strip()
        if where_sql.startswith("and"):
            where_sql = where_sql.replace("and", "", 1)
        where_sql = where_sql.strip()
        return where_sql, select_map

    def __construct_select_sql(self, select_conditions, select_map):
        '''
        根据之前得到的 select 条件列表和辅助 select 映射，构造 sql 中的 select 子句
        :param select_conditions:
        :param select_map:
        :return: e.g. a.s, b.s
        '''
        select_sql = ", ".join([select_map[select_condition] for select_condition in select_conditions])
        return select_sql


if __name__ == '__main__':
    # sparql_list = None
    # folder = "../res/watdiv/sparql/"
    # files = os.listdir(folder)
    # for file in files:
    #     with open(folder + file, "r") as f:
    #         # sparql_list = [x.strip() for x in f.read().split("#end") if x.strip()]
    #         sparql_list = re.findall("SELECT.*?\{.*?\}", f.read(), re.S)
    #     sparql2sql = Sparql2sql("t0", False)
    #     with open("../res/watdiv/sql/" + file, "w") as f:
    #         for sparql in sparql_list:
    #             sql = sparql2sql.transform(sparql)
    #             f.write(sql + "\n")

    with open("../res/yago/workload_sparql_x10", "r") as f:
        sparql_list = re.findall("SELECT.*?\{.*?\}", f.read(), re.S)
    sparql2sql = Sparql2sql("t0", True)
    f = open("../res/yago/workload_sql_x10", "w")
    for i, sparql in enumerate(sparql_list):
        # print(i+1)
        sql = sparql2sql.transform(sparql)
        sql = sql.replace("rdf:type", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        ub_search = re.findall("'ub:(.*?)'", sql)
        for item in ub_search:
            sql = sql.replace("ub:"+item, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#{0}".format(item))
        f.write(sql)
        f.write("\n")
    f.close()
