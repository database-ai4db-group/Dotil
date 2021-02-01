"""
author: zhr
将DBpedia Log转化为可读的查询。
"""
import re


def parse(input_path, output_path):
    f = open(input_path, "r", encoding="utf8")
    fw = open(output_path, "w", encoding="utf8")
    line = f.readline()
    while line:
        result = re.search("&query=([^&]*)", line)
        if result:
            ascii_query = result.group(1)

            def _sub_ascii(matched):
                ascii_num = matched.group(1)
                return chr(int(ascii_num, 16))

            query = re.sub("%([a-zA-Z0-9][0-9A-Fa-f])", _sub_ascii, ascii_query)
            fw.write(query + "\n")
        line = f.readline()
    f.close()
    fw.close()


def reform(input_path, output_path):
    f = open(input_path, "r", encoding='utf8')
    fw = open(output_path, "w", encoding="utf8")
    line = f.readline()
    while line:
        print(line)
        if line == "\n":
            line = f.readline()
            continue
        query = re.sub("\\+", " ", line)
        fw.write(query)
        line = f.readline()
    f.close()
    fw.close()


def reform_query(input_path, output_path):
    f = open(input_path, "r", encoding='utf8')
    fw = open(output_path, "w", encoding="utf8")
    line = f.readline()
    while line:
        print(line)
        if "SELECT" in line and "FORM" not in line:
            line = line.replace("\n", "").replace("\r", "") + " " + f.readline()
        if "SELECT" in line and "WHERE {" not in line:
            line = line.replace("\n", "").replace("\r", "") + " " + f.readline()
        while "SELECT" in line and "}" not in line:
            line = line.replace("\n", "").replace("\r", "") + " " + f.readline()
        fw.write(line)
        line = f.readline()
    f.close()
    fw.close()


if __name__ == '__main__':
    log_num = 12
    parse("WorkloadLogs/Original/log_" + str(log_num) + ".txt", "WorkloadLogs/Reform/query_" + str(log_num) + ".txt")
    reform("WorkloadLogs/Reform/query_" + str(log_num) + ".txt", "WorkloadLogs/Reform/reformed_" + str(log_num) + ".txt")
    # reform_query("WorkloadLogs/Reform/reformed_2.txt", "WorkloadLogs/reformed_query.txt")
