"""
author: zhr
处理Linked Geo Data (LGD) 数据集。
"""


def cut_tail():
    """减掉LGD数据的多余尾部数据"""
    f = open("D:\\ChromeDownload\\LGD\\lgd\\lgd.nt", "r", encoding="utf8")
    fw = open("D:\\ChromeDownload\\LGD\\lgd\\lgd_ct.nt", "w", encoding="utf8")
    line = f.readline()
    while line:
        split_strings = line.split("^^")
        if len(split_strings) == 1:
            fw.write(line)
        else:
            fw.write(split_strings[0] + " .\n")
        line = f.readline()
    f.close()
    fw.close()


if __name__ == '__main__':
    cut_tail()
