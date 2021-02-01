"""
author: zhr
重新组织数据文件：小数据集Yago和Bio2rdf。
"""
import re


def process_yago():
    f = open("yagoData.csv", "r", encoding="utf8")
    fw = open("yagoData.nt", "w", encoding="utf8")
    line = f.readline()
    while line:
        result = re.search("([^\t]*)\t([^\t]*)\t([^\t\n]*)", line)
        if result:
            s = "<http://db.org/source/" + result.group(1) + ">"
            p = "<http://db.org/source/" + result.group(2) + ">"
            o = "<http://db.org/source/" + result.group(3) + ">"
            fw.write(s + " " + p + " " + o + " .\n")
        line = f.readline()
    f.close()
    fw.close()


def process_bio2rdf():
    f = open("D:\\research_data\\bio2rdf_8g\\bio2rdf2.csv", "r", encoding="utf8")
    fw = open("D:\\research_data\\bio2rdf_8g\\bio2rdf.nt", "w", encoding="utf8")
    line = f.readline()
    while line:
        line = line.replace("\n", "").replace(" ", "").replace("\r", "")
        result = re.search("([^\t ]*)\t([^\t ]*)\t([^\t\n ]*)", line)
        if result:
            o = result.group(3)
            if re.match("<[^<>]*>", o):
                if "http" not in o:
                    o = o.replace("\"", "").replace("\\", "").replace("<", "").replace(">", "")
                    fw.write(result.group(1) + " " + result.group(2) + " \"" + o + "\" .\n")
                else:
                    fw.write(line.replace("\t", " ").replace("\"", "").replace("\\", "") + " .\n")
            elif re.match("\"[^\"]\"", o):
                fw.write(line.replace("\t", " ") + " .\n")
            else:
                fw.write(result.group(1) + " " + result.group(2) + " \"" + o + "\" .\n")
        line = f.readline()
    f.close()
    fw.close()


def transfer_unicode():
    f = open("D:\\research_data\\bio2rdf_8g\\bio2rdf2.csv", "r", encoding="utf8")
    fw = open("D:\\research_data\\bio2rdf_8g\\bio2rdf.nt", "w", encoding="utf8")
    line = f.readline()
    while line:
        line = line.replace("\n", "").replace(" ", "").replace("\r", "")
        result = re.search("([^\t ]*)\t([^\t ]*)\t([^\t\n ]*)", line)
        if result:
            s = "<" + result.group(1).replace('"', '').replace("<", "").replace(">", "") + ">"
            o = result.group(3)
            if re.match("<.*>", o):
                o = "<" + o.replace("<", "").replace(">", "") + ">"
                if "http" not in o:
                    o = o.replace("\"", "").replace("\\", "").replace("<", "").replace(">", "")
                    fw.write(s + " " + result.group(2) + " \"" + o + "\" .\n")
                else:
                    line = s + " " + result.group(2) + " " + o
                    fw.write(line.replace("\t", " ").replace("\"", "").replace("\\", "") + " .\n")
            elif re.match("\"[^\"]\"", o):
                line = s + " " + result.group(2) + " " + o
                fw.write(line.replace("\t", " ") + " .\n")
            else:
                fw.write(s + " " + result.group(2) + " \"" + o + "\" .\n")
        line = f.readline()
    f.close()
    fw.close()


if __name__ == '__main__':
    transfer_unicode()
