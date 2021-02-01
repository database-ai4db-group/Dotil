import os
"""
把所有分散的nt文件组合到一起
"""


def merge_nt(dir_path, output_path):
    fw = open(output_path, "w", encoding="utf8")
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if "nt" in file:
                print(file)
                with open(root + "\\" + file, "r", encoding="utf8") as f:
                    line = f.readline()
                    count = 0
                    while line:
                        fw.write(line)
                        line = f.readline()
                        count += 1
                        if count % 100000 == 0:
                            print(count)


if __name__ == '__main__':
    merge_nt("D:\\ChromeDownload\\LGD\\lgd", "D:\\ChromeDownload\\LGD\\lgd\\lgd.nt")
