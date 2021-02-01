"""
author: zhr
计数文件的triples、SUO等数据。
"""


def count_triples():
    f = open("D:\\ChromeDownload\\LGD\\lgd\\lgd_ct.nt", "r", encoding="utf8")
    line = f.readline()
    count = 0
    while line:
        count += 1
        line = f.readline()
    f.close()
    print("Count: " + str(count))

def count_suo():
    suo_set = set()
    f = open("D:\\ChromeDownload\\LGD\\lgd\\lgd_ct.nt", "r", encoding="utf8")
    line = f.readline()
    count = 0
    while line:
        count += 1
        split = line.rstrip(" .").replace("\n", "").replace("\r", "").split("\t")
        s = split[0]
        o = split[2]
        if s not in suo_set:
            suo_set.add(s)
        if o not in suo_set:
            suo_set.add(o)
        line = f.readline()
        # if count % 100000 == 0:
        #     print(suo_set)
    f.close()
    print("suo Count:" + str(len(suo_set)))



if __name__ == '__main__':
    count_suo()
