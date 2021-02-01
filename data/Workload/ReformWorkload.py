"""
author: zhr
重构Workload SQL<->SPARQL
"""
import re


def _reform_sql2sparql(query):
    """从SQL到SPARQL"""
    parse_result = re.search("SELECT (.*) FROM (.*) WHERE ([^\n\r]*)", query, flags=re.I)
    select_clause = parse_result.group(1)
    from_clause = parse_result.group(2)
    where_clause = parse_result.group(3)
    # 首先了解有几条语句
    split_from = from_clause.split(",")
    line_length = len(split_from)
    # 初始化标号表
    line_numbers = [[i, i+1] for i in range(0, line_length, 2)]
    # 查找p
    p_list = re.findall("[a-zA-Z]\\.pname *= *[\'|\"]([^ ]*)[\'|\"]", where_clause, re.I)
    equals_list = re.findall("([a-zA-Z])\\.([s|o])name *= *([a-zA-Z])\\.([s|o])name", where_clause, re.I)
    const_list = re.findall("([a-zA-Z])\\.([s|o])name *= *[\'|\"]([^ ]*)[\'|\"]", where_clause, re.I)
    lines = []
    for i in range(line_length):
        lines.append({"s": "", "p": "", "o": ""})
    # 固定P
    # for p_num in range(len(p_list)):
    #     lines[p_num]["p"] = "<http://db.org/source/" + p_list[p_num] + ">"
    for p_num in range(len(p_list)):
        lines[p_num]["p"] = p_list[p_num]
    so_dict = {"s": 0, "o": 1}
    # 处理常量
    # for tuple in const_list:
    #     lines[int(ord(tuple[0]) - 97)][tuple[1]] = "<http://db.org/source/" + tuple[2] + ">"
    for tuple in const_list:
        lines[int(ord(tuple[0]) - 97)][tuple[1]] = tuple[2]
    # 处理变量
    # 先初始化所有的s和o，用字母排序abc+s|o进行默认命名
    for p_dict_num in range(len(lines)):
        p_dict = lines[p_dict_num]
        for triple in p_dict:
            if p_dict[triple] == "":
                p_dict[triple] = "?" + chr(p_dict_num + 97) + triple
    # 对所有相等的都划分到小的那一部分上
    for tuple in equals_list:
        line1 = tuple[0]
        line1_so = tuple[1]
        line2 = tuple[2]
        line2_so = tuple[3]
        line1_num = ord(line1) - 97
        line2_num = ord(line2) - 97
        if line1_num < line2_num:
            lines[line2_num][line2_so] = lines[line1_num][line1_so]
        else:
            lines[line1_num][line1_so] = lines[line2_num][line2_so]
    # SELECT部分
    selects = re.findall("([a-zA-Z])\\.([s|o])name", select_clause, re.I)
    sparql_string = "SELECT "
    for select in selects:
        sparql_string += lines[int(ord(select[0]) - 97)][select[1]] + " "
    sparql_string += "WHERE{ "
    for line in lines:
        sparql_string += line["s"] + " " + line["p"] + " " + line["o"] + " .   "
    sparql_string += "}"
    return sparql_string


def _reform_sql2gremlin(query):
    parse_result = re.search("SELECT (.*) FROM (.*) WHERE ([^\n\r]*)", query, flags=re.I)
    select_clause = parse_result.group(1)
    from_clause = parse_result.group(2)
    where_clause = parse_result.group(3)
    # 首先了解有几条语句
    split_from = from_clause.split(",")
    line_length = len(split_from)
    # 初始化标号表
    line_numbers = [[i, i + 1] for i in range(0, line_length, 2)]
    # 查找p
    p_list = re.findall("[a-zA-Z]\\.pname *= *[\'|\"]([^ ]*)[\'|\"]", where_clause, re.I)
    equals_list = re.findall("([a-zA-Z])\\.([s|o])name *= *([a-zA-Z])\\.([s|o])name", where_clause, re.I)
    const_list = re.findall("([a-zA-Z])\\.([s|o])name *= *[\'|\"]([^ ]*)[\'|\"]", where_clause, re.I)
    lines = []
    for i in range(line_length):
        lines.append({"s": "", "p": "", "o": ""})
    # 固定P
    # for p_num in range(len(p_list)):
    #     lines[p_num]["p"] = "<http://db.org/source/" + p_list[p_num] + ">"
    for p_num in range(len(p_list)):
        lines[p_num]["p"] = p_list[p_num]
    so_dict = {"s": 0, "o": 1}
    # 处理常量
    # for tuple in const_list:
    #     lines[int(ord(tuple[0]) - 97)][tuple[1]] = "<http://db.org/source/" + tuple[2] + ">"
    for tuple in const_list:
        lines[int(ord(tuple[0]) - 97)][tuple[1]] = tuple[2]
    # 处理变量
    # 先初始化所有的s和o，用字母排序abc+s|o进行默认命名
    for p_dict_num in range(len(lines)):
        p_dict = lines[p_dict_num]
        for triple in p_dict:
            if p_dict[triple] == "":
                p_dict[triple] = chr(p_dict_num + 97) + triple
    # 对所有相等的都划分到小的那一部分上
    for tuple in equals_list:
        line1 = tuple[0]
        line1_so = tuple[1]
        line2 = tuple[2]
        line2_so = tuple[3]
        line1_num = ord(line1) - 97
        line2_num = ord(line2) - 97
        if line1_num < line2_num:
            lines[line2_num][line2_so] = lines[line1_num][line1_so]
        else:
            lines[line1_num][line1_so] = lines[line2_num][line2_so]
    selects = re.findall("([a-zA-Z])\\.([s|o])name", select_clause, re.I)
    # 重组Gremlin
    gremlin = "g.V().match("
    for line in lines:
        gremlin += 'as("' + line["s"] + '").out("' + line["p"] + '").as("' + line["o"] + "),"
    gremlin = gremlin.rstrip(",") + ").select("
    for select in selects:
        gremlin += '"' + lines[int(ord(select[0]) - 97)][select[1]] + '", '
    gremlin = gremlin.rstrip(", ") + ')'
    return gremlin


def reform(source, target):
    with open(source, "r", encoding="utf8") as f:
        with open(target, "w", encoding="utf8") as fw:
            sparqls = []
            line = f.readline()
            while line:
                sparqls.append(_reform_sql2sparql(line))
                fw.write(_reform_sql2sparql(line) + "\n")
                line = f.readline()
            print(sparqls)


def split_workload():
    """将一个整个的workload文件分成每一个SPARQL一个文件"""
    f = open("Workload/bio2rdf_sparql.txt", "r", encoding="utf8")
    lines = f.readlines()
    f.close()
    for line_num in range(len(lines)):
        with open("Workload/split/bio2rdf_sparql_" + str(line_num + 1) + ".txt", "w", encoding="utf8") as fw:
            fw.write(lines[line_num])


if __name__ == '__main__':
    # reform("Workload/bio2rdf_mysql.txt", "Workload/bio2rdf_sparql.txt")
    # split_workload()
    query = _reform_sql2gremlin("SELECT a.oname, b.oname FROM yagoData AS a, yagoData AS b, yagoData AS c, yagoData AS d, yagoData AS e WHERE a.pname='hasGivenName' AND b.pname='hasFamilyName' AND c.pname='wasBornIn' AND d.pname='hasAcademicAdvisor' AND e.pname='wasBornIn' AND a.sname=b.sname AND a.sname=c.sname AND a.sname=d.sname AND c.oname = e.oname AND d.oname=e.sname")
    print(query)







