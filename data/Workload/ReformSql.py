"""
author: zhr
重组查询Workload。
"""
import re


def reform_sql(source, target):
    """SPARQL到SQL"""
    p_sum = dict()
    p_index = dict()
    sql_list = []
    with open("D:\\ChromeDownload\\LGD\\lgd\\data\\lgd_p_sum.txt", "r", encoding="utf8") as fs:
        line = fs.readline()
        while line:
            result = re.search("([0-9]*)\t[0-9]*\t([^\t]*)", line)
            if result:
                num = result.group(1) # String
                all_p = result.group(2).split(",")
                p_nums = []
                for p in all_p:
                    if ")" in p:
                        continue
                    else:
                        p_nums.append(p.replace("(", "").replace("[", ""))
                p_sum[num] = p_nums
            line = fs.readline()
    with open("D:\\ChromeDownload\\LGD\\lgd\\lgd_p_count.txt", "r", encoding="utf8") as fw:
        line = fw.readline()
        while line:
            result = re.search("([^\t]*)\t([^\t\n]*)", line)
            if result:
                p_index[result.group(1)] = result.group(2)
            line = fw.readline()

    f = open(source, "r", encoding="utf8")
    line = f.readline()
    while line:
        if re.match("[0-9]*\n", line):
            query_list = []
            if line == "20\n":
                print("20")
            while line != "}\n":
                line = f.readline()
                query_list.append(line)
            query_list.remove("}\n")
            result = re.search("SELECT (.*) WHERE", query_list[0], flags=re.I)
            ret_values = result.group(1)  # 取得返回的值,可能是由变量组成的，也可能是*
            split_query = []
            p_list = []
            for query_line in query_list[1:]:
                line_result = re.search("([^ ]*) ([^ ]*) ([^ \n]*)", query_line)
                if line_result:
                    s = line_result.group(1)
                    p = line_result.group(2)
                    o = line_result.group(3).rstrip(".")
                    split_query.append((s, o))
                    p_list.append(p)
                else:
                    raise Exception("在匹配每一行时失败!")
            sql_and = ""
            const_and = ""
            # 判断每两句之间的连接
            for i in range(len(split_query)):
                for j in range(i + 1, len(split_query)):
                    if split_query[i][0] == split_query[j][0] and split_query[i][0][0] == "?":
                        sql_and += chr(97 + i) + ".sname=" + chr(97 + j) + ".sname and "
                    if split_query[i][1] == split_query[j][0] and split_query[i][1][0] == "?":
                        sql_and += chr(97 + i) + ".oname=" + chr(97 + j) + ".sname and "
                    if split_query[i][0] == split_query[j][1] and split_query[i][0][0] == "?":
                        sql_and += chr(97 + i) + ".sname=" + chr(97 + j) + ".oname and "
                    if split_query[i][1] == split_query[j][1] and split_query[i][1][0] == "?":
                        sql_and += chr(97 + i) + ".oname=" + chr(97 + j) + ".oname and "
                # 常量
                if split_query[i][0][0] != "?":
                    const_and += chr(97 + i) + ".sname='" + split_query[i][0] + "' and "
                if split_query[i][1][0] != "?":
                    const_and += chr(97 + i) + ".oname='" + split_query[i][1] + "' and "
            const_and = const_and[:-4]
            # P
            if len(const_and) == 0:
                sql_and = sql_and[:-4]
            p_and = ""
            for line_num in range(len(p_list)):
                p_and += chr(97 + line_num) + ".pname='" + p_list[line_num] + "' and "
            # 返回值
            # ret_values = re.sub(" *", "", ret_values)
            ret_string = ""
            if "*" in ret_values:
                var_so = set()
                for query in split_query:
                    if query[0][0] == "?":
                        var_so.add(query[0])
                    if query[1][0] == "?":
                        var_so.add(query[1])
                for variable in var_so:
                    for i in range(len(split_query)):
                        if variable == split_query[i][0]:
                            ret_string += chr(97 + i) + ".sname,"
                            break
                        if variable == split_query[i][1]:
                            ret_string += chr(97 + i) + ".oname,"
                            break
            else:
                variables = ret_values.split(" ")
                for variable in variables:
                    variable = variable.strip(" ")
                    for i in range(len(split_query)):
                        if variable == split_query[i][0]:
                            ret_string += chr(97 + i) + ".sname,"
                            break
                        if variable == split_query[i][1]:
                            ret_string += chr(97 + i) + ".oname,"
                            break
            ret_string = ret_string.rstrip(",")

            # From部分
            from_string = " FROM "
            for p_list_num in range(len(p_list)):
                if p_list[p_list_num] not in p_index:
                    p_order = "3955"
                    print(p_list[p_list_num])
                else:
                    p_order = p_index[p_list[p_list_num]]
                for order_sum in p_sum:
                    for sum_num in p_sum[order_sum]:

                        if int(p_order) == int(sum_num):
                            from_string += "lgd" + order_sum + " as " + chr(97 + p_list_num) + ", "
                            break

            from_string = from_string.rstrip(", ")
            sql = "SELECT " + ret_string + from_string + " WHERE " + p_and + sql_and + const_and + ";"
            sql_list.append(sql)
            with open(target, 'a', encoding="utf8") as fwd:
                fwd.write(sql + "\n")
        line = f.readline()
        print(line)
        print(sql_list)
    f.close()


def reform_cypher(source, target):
    """将SPARQL转化为Cypher"""
    cyphers = []
    f = open(source, "r", encoding="utf8")
    fw = open(target, "w", encoding="utf8")
    line = f.readline()
    while line:
        if re.match("[0-9]*\n", line):
            cypher = "MATCH "
            query_list = []
            while line != "}\n":
                line = f.readline()
                query_list.append(line)
            query_list.remove("}\n")
            result = re.search("SELECT (.*) WHERE", query_list[0], flags=re.I)
            ret_values = result.group(1)  # 取得返回的值,可能是由变量组成的，也可能是*
            split_query = []
            p_list = []
            for query_line in query_list[1:]:
                line_result = re.search("([^ ]*) ([^ ]*) ([^ \n]*)", query_line)
                if line_result:
                    s = line_result.group(1)
                    p = line_result.group(2)
                    o = line_result.group(3).rstrip(".")
                    split_query.append((s, o))
                    p_list.append(p)
                else:
                    raise Exception("在匹配每一行时失败!")
            var_rets = set()
            for i in range(len(split_query)):
                if split_query[i][0][0] == "?":
                    var_rets.add(split_query[i][0].lstrip("?"))
                    cypher += "((" + split_query[i][0].replace("?", "") + ":subject)"
                else:
                    cypher += "((:subject{name:'" + split_query[i][0] + "'})"
                cypher += "-[:predicate{type:'" + p_list[i] + "'}]->"
                if split_query[i][1][0] == "?":
                    var_rets.add(split_query[i][1].lstrip("?"))
                    cypher += "(" + split_query[i][1].replace("?", "") + ":object))"
                else:
                    cypher += "(:object{name:'" + split_query[i][1] + "'})),"
            cypher = cypher.rstrip(",")
            cypher += " RETURN "
            if "*" in ret_values:
                for value in var_rets:
                    cypher += value + ".name, "
                cypher = cypher.rstrip(", ")
            else:
                ret_values_split = ret_values.split(" ")
                for value in ret_values_split:
                    value = value.strip(" ")
                    value = re.sub("\\?", "", value)
                    cypher += value + ".name, "
                cypher = cypher.rstrip(", ")
            cyphers.append(cypher)
            print(cypher)
        line = f.readline()
    for cypher in cyphers:
        fw.write(cypher + "\n")
    print("write over")
    fw.close()
    f.close()


if __name__ == '__main__':
    reform_sql("Final/lgd/final_query.txt", "Final/lgd/SQL/lgd_sql_query.txt")
    # reform_sql("Final/dbpedia/final_test.txt", "Final/dbpedia/SQL/dbpedia_sql_test.txt")
    # reform_cypher("Final/lgd/final_query.txt", "Final/lgd/Cypher/lgd_cypher_query.txt")











