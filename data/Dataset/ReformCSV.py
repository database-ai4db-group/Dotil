"""
author: zhr
处理小数据集的CSV文件
"""


def reform(nt_path, csv_path):
    f = open(nt_path, "r", encoding="utf8")
    fw = open(csv_path, "w", encoding="utf8")
    line = f.readline()
    count = 0
    while line:
        split_line = line.split(" ")
        if len(split_line) > 4:
            w_line = split_line[0] + "\t" + split_line[1] + "\t"
            for i in range(2, len(split_line)):
                if i == len(split_line) - 1:
                    continue
                else:
                    w_line += split_line[i] + ' '
            w_line.rstrip(" ")
            w_line += "\n"
        else:
            w_line = split_line[0] + "\t" + split_line[1] + "\t" + split_line[2] + "\n"
        fw.write(w_line)
        line = f.readline()
        count += 1
        if count % 100000 == 0:
            print(count)
            print(line)
    print("Total Count: " + str(count))


def reform_csv2nebula_form():
    f = open("D:\\research_data\\bio2rdf_8g\\bio2rdf2.csv", "r", encoding="utf8")
    fw1 = open("D:\\research_data\\bio2rdf_8g\\bio2rdf_node.csv", "w", encoding="utf8")
    fw2 = open("D:\\research_data\\bio2rdf_8g\\bio2rdf_relation.csv", "w", encoding="utf8")
    line = f.readline()
    s_ids = set()
    while line:
        line = line.replace("\r", "").replace("\n", "").replace("\"", "").replace("\\", "").replace("'", "")
        split = line.split("\t")
        s_id = str(hash(split[0]))
        o_id = str(hash(split[2]))
        if s_id not in s_ids:
            fw1.write(s_id + "\t" + split[0] + "\n")
            s_ids.add(s_id)
        if o_id not in s_ids:
            fw1.write(o_id + "\t" + split[2] + "\n")
            s_ids.add(o_id)
        fw2.write(s_id + "\t" + o_id + "\t" + split[1] + "\n")
        line = f.readline()
    f.close()
    fw1.close()
    fw2.close()


def reform_csv2hugegraph_form():
    f = open("D:\\research_data\\yago_670MB\\yagoData\\yagoData.csv", "r", encoding="utf8")
    fw1 = open("D:\\research_data\\yago_670MB\\yagoData\\yago_node.csv", "w", encoding="utf8")
    fw2 = open("D:\\research_data\\yago_670MB\\yagoData\\yago_rela.json", "w", encoding="utf8")
    line = f.readline()
    s_ids = set()
    while line:
        line = line.replace("\r", "").replace("\n", "").replace("\"", "").replace("\\", "").replace("'", "").replace(",", "")
        split = line.split("\t")
        s_id = str(hash(split[0]))
        o_id = str(hash(split[2]))
        if s_id not in s_ids:
            fw1.write(split[0][:120] + "\n")
            s_ids.add(s_id)
        if o_id not in s_ids:
            fw1.write(split[2][:120] + "\n")
            s_ids.add(o_id)
        fw2.write('{"source_name": "' + split[0][:120] + '", "target_name": "' + split[2][:120] + '", "name": "' + split[1][:120] + '"}\n')
        line = f.readline()
    f.close()
    fw1.close()
    fw2.close()


def reform_csv2neo4j_form():
    f = open("D:\\research_data\\bio2rdf_8g\\bio2rdf2.csv", "r", encoding="utf8")
    fw1 = open("D:\\research_data\\bio2rdf_8g\\bio2rdf_node.csv", "w", encoding="utf8")
    fw2 = open("D:\\research_data\\bio2rdf_8g\\bio2rdf_relation.csv", "w", encoding="utf8")
    line = f.readline()
    fw1.write("nodeId:ID,name,:LABEL\n")
    fw2.write(":START_ID,:END_ID,:TYPE\n")
    s_ids = set()
    while line:
        line = line.replace("\r", "").replace("\n", "").replace("\"", "").replace("\\", "").replace("'", "").replace(
            ",", "")
        split = line.split("\t")
        s_id = str(hash(split[0]))
        o_id = str(hash(split[2]))
        if s_id not in s_ids:
            fw1.write(s_id + "," + split[0] + ",node\n")
            s_ids.add(s_id)
        if o_id not in s_ids:
            fw1.write(o_id + "," + split[2] + ",node\n")
            s_ids.add(o_id)
        fw2.write(s_id + "," + o_id + "," + split[1] + "\n")
        line = f.readline()
    f.close()
    fw1.close()
    fw2.close()


if __name__ == '__main__':
    # reform(sys.argv[1], sys.argv[2])
    reform_csv2neo4j_form()
