import re
import sys
import threading
import pymysql
import random
import time
import inspect
import ctypes
import configparser


class RDBView:
    def __init__(self):
        self.cf = configparser.ConfigParser()
        self.cf.read("config.ini", encoding="utf-8")
        self.host = self.cf.get("total", "host")
        self.port = int(self.cf.get("total", "port"))
        self.user = self.cf.get("total", "user")
        self.password = self.cf.get("total", "password")
        self.db = self.cf.get("total", "db")
        self.charset = self.cf.get("total", "charset")
        self.threshold = float(self.cf.get("rdb-view", "threshold"))  # view / mysql限制
        self.db_flag = self.cf.get("total", "db_flag")  # 选择测试哪个数据集
        self.view_total_number = 0
        self.query_number = 100
        self.views = dict()  # 保存view的dict
        self.view_asc_number = 0  # 每个view的编号
        self.mysql_total_number = int(self.cf.get("total", "triple_length"))
        self.delete_queue = []  # 删除的顺序队列
        self.every_view_number = dict()
        self.query_time = 0  # 记录每一个查询时间
        self.transfer_time = 0.0  # view建立时间
        self.rdb_total_time = 0.0  # 查询总时间
        self.view_actual = dict()  # 查询的具体语句
        f_p = open(self.cf.get("total", "p_record_dir") + self.cf.get("total", "dataset") + "_p_record.txt", 'r', encoding='utf8')
        self.p_number = dict()  # 这个query数据集每个p对应的元组数
        self.p_is_in_view = dict()  # 这个p是否已经在view中
        lines = f_p.readlines()
        for line in lines:
            result = re.search("([^\t]*)\t([^\t]*)", line)
            self.p_is_in_view[result.group(1)] = False
            self.p_number[result.group(1)] = result.group(2)
        self.breakdown = dict()  # 记录b) breakdown
        self.view_select = dict()
        self.this_time_result = []
        self.one_results = []
        print("initial success")

    def get_p(self, query):
        pattern = re.compile("\\.pname *= *\\'([^\\']*)\\'")
        p_list = pattern.findall(query)
        return p_list

    def rdb_query(self, query):
        db = pymysql.connect(host=self.host,
                             port=self.port,
                             user=self.user,
                             passwd=self.password,
                             db=self.db,
                             charset=self.charset)
        cursor = db.cursor()
        start = time.perf_counter()
        cursor.execute(query)
        result = cursor.fetchall()
        end = time.perf_counter()
        self.query_time = end - start
        print("MySQL Query Result Length: " + str(len(result)))
        db.close()

    def rdb_view_query(self, query, length):
        db = pymysql.connect(host=self.host,
                             port=self.port,
                             user=self.user,
                             passwd=self.password,
                             db=self.db,
                             charset=self.charset)
        self.one_results = []
        cursor = db.cursor()
        start = time.perf_counter()
        cursor.execute(query)
        end = time.perf_counter()
        print("View Query Over, Now Process Results")
        self.query_time = end - start
        results = cursor.fetchall()
        if len(results) == 0:
            print("MySQL View Query Result Length: 0")
            self.one_results = []
            return
        one_results = []
        print(results[:3])
        print(type(results[0]))
        for result in results:
            self.one_results.append(result[:length])
        print(self.one_results[:3])
        cursor.close()
        db.close()
        print("MySQL View Query Result Length: " + str(len(self.one_results)))

    def find_oname(self, p):
        # 将WHERE前后分开
        result_where = re.match(".*WHERE *(.*)", p)
        # 查找pname
        pname_pattern = re.compile("pname")
        pname_result = pname_pattern.findall(p)
        query_length = len(pname_result)  # 语句数
        number_list = []  # 建立标号表
        var_list = []  # 建立常量/变量表
        asc_number = 0
        for i in range(query_length):
            number_list.append([asc_number, asc_number + 1])
            var_list.append([False, False])
            asc_number += 2
        # 初始化标号表
        initial_pattern = re.compile("([a-z])\\.([a-z])name *= *([a-z])\\.([a-z])name")
        initial_result = initial_pattern.findall(p)
        initial_dict = {"s": 0, "o": 1}
        if initial_result:
            for result_tuple in initial_result:
                first_part = number_list[ord(result_tuple[0]) - 97][initial_dict[result_tuple[1]]]
                second_part = number_list[ord(result_tuple[2]) - 97][initial_dict[result_tuple[3]]]
                number_list[ord(result_tuple[0]) - 97][initial_dict[result_tuple[1]]] = min(first_part, second_part)
                number_list[ord(result_tuple[2]) - 97][initial_dict[result_tuple[3]]] = min(first_part, second_part)

        # 初始化常量变量表
        var_pattern = re.compile("([a-z])\\.([so])name *= *'[^']*'")
        var_result = var_pattern.findall(p)
        if var_result:
            for result_tuple in var_result:
                var_list[ord(result_tuple[0]) - 97][initial_dict[result_tuple[1]]] = True

        # start to choose sub query p
        oname_community = []  # 存储sub query的编号
        oname_community_letters = []
        for number_count in range(len(number_list)):
            row_flag = 0
            for row_count in range(2):
                if var_list[number_count][row_count]:
                    row_flag += 1
                else:
                    if number_list[number_count][
                        row_count] != 2 * number_count + row_count:  # 条件一：如果这个编号已经被修改了，那么说明有相同的值
                        row_flag += 1
                    else:
                        community_flag = False
                        for j in range(len(number_list)):  # 条件二：如果这个编号是所有的相同值中最小值，那么有可能它没有变化
                            if (number_list[j][1] == number_list[number_count][row_count] or
                                number_list[j][0] == number_list[number_count][row_count]) \
                                    and j != number_count:
                                community_flag = True
                        if community_flag:
                            row_flag += 1
            if row_flag == 2:  # add this p to new sub query
                oname_community.append(number_count)
                oname_community_letters.append(chr(number_count + 97))
        # 第三步是找到相应的p
        p_list = []
        for oname in oname_community_letters:
            result_p = re.search(oname + "\\.pname *= *'([^ ]*)'", result_where.group(1))
            p_list.append(result_p.group(1))
        # if don't have sub query,then stop and return False
        if len(oname_community) == 0:
            print("Don't have sub query")
            return False, None, None, None, None
        if len(oname_community) == query_length:
            # 全是子结构
            print("This whole query is a sub query.")
            return True, p_list, p, "", 0

        # 提取所有的语句编号
        pattern2 = re.compile('([a-z]).pname')
        result_all_word_letter = pattern2.findall(result_where.group(1))
        others_in_query = set(result_all_word_letter).difference(set(oname_community_letters))  # 非子查询的编号字母

        # add new part
        first_class = []
        second_class = []
        third_class = []

        # judge first\third class
        for oname in oname_community:
            s_flag = False
            o_flag = False
            for word in others_in_query:

                if number_list[ord(word) - 97][0] == number_list[oname][0] or \
                        number_list[ord(word) - 97][1] == number_list[oname][0]:
                    third_class.append(number_list[oname][0])
                    s_flag = True
                if number_list[ord(word) - 97][0] == number_list[oname][1] or \
                        number_list[ord(word) - 97][1] == number_list[oname][1]:
                    third_class.append(number_list[oname][1])
                    o_flag = True
            if not s_flag:
                first_class.append(number_list[oname][0])
            if not o_flag:
                first_class.append(number_list[oname][1])
        # judge second class
        for word in others_in_query:
            s_flag = False
            o_flag = False
            for oname in oname_community:
                if number_list[oname][0] == number_list[ord(word) - 97][0] or \
                        number_list[oname][1] == number_list[ord(word) - 97][0]:
                    s_flag = True
                if number_list[oname][0] == number_list[ord(word) - 97][1] or \
                        number_list[oname][1] == number_list[ord(word) - 97][1]:
                    o_flag = True
            if not s_flag:
                second_class.append(number_list[ord(word) - 97][0])
            if not o_flag:
                second_class.append(number_list[ord(word) - 97][1])
        first_class = list(set(first_class))
        second_class = list(set(second_class))
        third_class = list(set(third_class))
        third_class.sort()
        second_class.sort()
        first_class.sort()

        # judge select number list
        # 取出select部分
        select_part_result = re.search("SELECT (.*) FROM", p)
        select_part = select_part_result.group()
        select_part_pattern = re.compile("([a-z])\\.([a-z])name")
        select_part_all_result = select_part_pattern.findall(select_part)  # SELECT 部分的字母
        select_number_list = []
        for value in select_part_all_result:
            if value[1] == 's':
                select_number_list.append(number_list[ord(value[0]) - 97][0])
            else:
                select_number_list.append(number_list[ord(value[0]) - 97][1])

        connection_point = []
        connection_point.extend(third_class)
        # add first to other select part
        first_select_number_list = []
        for value in first_class:
            if value in select_number_list:
                first_select_number_list.append(value)

        # sub mysql query select part
        sub_mysql_select_part = []
        sub_mysql_select_part.extend(connection_point)
        sub_mysql_select_part.extend(first_select_number_list)
        # 7.1 清除原query的非子结构部分:FROM和WHERE部分
        sub_mysql = p  # 新的子查询
        for word_letter in others_in_query:
            sub_mysql = re.sub(", *[a-zA-Z0-9]* AS " + str(word_letter), "", sub_mysql)
            sub_mysql = re.sub("FROM *[a-zA-Z0-9]* AS " + str(word_letter) + " *, *", "FROM ", sub_mysql)
            sub_mysql = re.sub("WHERE *" + str(word_letter) + "\\.pname *= *[^ ]* *AND", "WHERE ", sub_mysql)
            sub_mysql = re.sub("AND *" + str(word_letter) + "\\.pname *= *[^ ]*", "", sub_mysql)
            sub_mysql = re.sub("[AND]* *" + str(word_letter) + "\\.oname *= *([^= ]*)", "", sub_mysql)
            sub_mysql = re.sub("[AND]* *" + str(word_letter) + "\\.sname *= *([^= ]*)", "", sub_mysql)
        sub_mysql = re.sub(" *[AND]* [a-z]\\.[a-z]name *= *[a-z]\\.[a-z]name", "", sub_mysql)  # 除去所有连接语句
        sub_mysql = re.sub("WHERE *AND", "WHERE ", sub_mysql)

        # 创建新的连接语句
        already_build = set()  # 重复的连接句要去掉
        for number in oname_community:
            sname_number = number_list[number][0]
            oname_number = number_list[number][1]
            for i in range(len(number_list)):
                if i == number:
                    continue
                if chr(i + 97) in others_in_query:
                    continue
                if number_list[i][0] == sname_number and (number, 0, i, 0) not in already_build:
                    sub_mysql += " AND " + str(chr(number + 97)) + ".sname=" + str(chr(i + 97)) + ".sname"
                    already_build.add((number, 0, i, 0))
                    already_build.add((i, 0, number, 0))
                elif number_list[i][1] == sname_number and (number, 0, i, 1) not in already_build:
                    sub_mysql += " AND " + str(chr(number + 97)) + ".sname=" + str(chr(i + 97)) + ".oname"
                    already_build.add((number, 0, i, 1))
                    already_build.add((i, 1, number, 0))
                if number_list[i][0] == oname_number and (number, 1, i, 0) not in already_build:
                    sub_mysql += " AND " + str(chr(number + 97)) + ".oname=" + str(chr(i + 97)) + ".sname"
                    already_build.add((number, 1, i, 0))
                    already_build.add((i, 0, number, 1))
                elif number_list[i][1] == oname_number and (number, 1, i, 1) not in already_build:
                    sub_mysql += " AND " + str(chr(number + 97)) + ".oname=" + str(chr(i + 97)) + ".oname"
                    already_build.add((number, 1, i, 1))
                    already_build.add((i, 1, number, 1))

        # 设置子查询的SELECT部分：SELECT部分
        # 先构建SELECT的替换语句,其中连接点表已经从小到大排序
        select_exchange = []
        for point in sub_mysql_select_part:
            for i in range(len(number_list)):
                if i not in oname_community:
                    continue
                if number_list[i][0] == point:
                    select_exchange.append((i, 's'))
                    break
                elif number_list[i][1] == point:
                    select_exchange.append((i, 'o'))
                    break

        select_string = ""
        for exchange in select_exchange:
            exchange_letter = chr(int(exchange[0]) + 97)
            exchange_name = str(exchange[1]) + 'name'
            select_string += str(exchange_letter) + "." + str(exchange_name) + ", "
        select_string = select_string.rstrip(", ")
        # 替换语句
        sub_mysql = re.sub("SELECT (.*) FROM", "SELECT " + select_string + " FROM", sub_mysql)

        # 生成mysql非子查询部分，用于最终查询
        # 将所有子查询部分去除
        other_mysql = p  # 新的子查询
        for word_letter in oname_community_letters:
            other_mysql = re.sub(", *[a-zA-Z0-9]* AS " + str(word_letter), "", other_mysql)
            other_mysql = re.sub("FROM *[a-zA-Z0-9]* AS " + str(word_letter) + " *, ", "FROM ", other_mysql)
            other_mysql = re.sub("AND " + str(word_letter) + "\\.[a-z]name *= *[a-z]\\.[a-z]name", "", other_mysql)
            other_mysql = re.sub("WHERE *" + str(word_letter) + "\\.[a-z]name *= *[a-z]\\.[a-z]name", "WHERE",
                                 other_mysql)
            other_mysql = re.sub("AND *[a-z]\\.[a-z]name *= *" + str(word_letter) + "\\.[a-z]name", "", other_mysql)
            other_mysql = re.sub("WHERE *[a-z]\\.[a-z]name *= *" + str(word_letter) + "\\.[a-z]name", "WHERE",
                                 other_mysql)
            other_mysql = re.sub("[AND]* *" + str(word_letter) + "\\.pname *= *([^= ]*)", "", other_mysql)
            other_mysql = re.sub("[AND]* *" + str(word_letter) + "\\.oname *= *([^= ]*)", "", other_mysql)
            other_mysql = re.sub("[AND]* *" + str(word_letter) + "\\.sname *= *([^= ]*)", "", other_mysql)
        other_mysql = re.sub("WHERE *AND", "WHERE ", other_mysql)
        other_mysql = re.sub(" *[AND]* [a-z]\\.[a-z]name *= *[a-z]\\.[a-z]name", "", other_mysql)  # remove the relation
        # 创建新的连接语句
        already_build = set()  # 重复的连接句要去掉
        for letter in others_in_query:
            number = ord(letter) - 97
            sname_number = number_list[number][0]
            oname_number = number_list[number][1]
            for i in range(len(number_list)):
                if i == number:
                    continue
                if chr(i + 97) in oname_community_letters:
                    continue
                if number_list[i][0] == sname_number and (number, 0, i, 0) not in already_build:
                    other_mysql += " AND " + str(chr(number + 97)) + ".sname=" + str(chr(i + 97)) + ".sname"
                    already_build.add((number, 0, i, 0))
                    already_build.add((i, 0, number, 0))
                elif number_list[i][1] == sname_number and (number, 0, i, 1) not in already_build:
                    other_mysql += " AND " + str(chr(number + 97)) + ".sname=" + str(chr(i + 97)) + ".oname"
                    already_build.add((number, 0, i, 1))
                    already_build.add((i, 1, number, 0))
                if number_list[i][0] == oname_number and (number, 1, i, 0) not in already_build:
                    other_mysql += " AND " + str(chr(number + 97)) + ".oname=" + str(chr(i + 97)) + ".sname"
                    already_build.add((number, 1, i, 0))
                    already_build.add((i, 0, number, 1))
                elif number_list[i][1] == oname_number and (number, 1, i, 1) not in already_build:
                    other_mysql += " AND " + str(chr(number + 97)) + ".oname=" + str(chr(i + 97)) + ".oname"
                    already_build.add((number, 1, i, 1))
                    already_build.add((i, 1, number, 1))

        other_select_result = re.search("SELECT .* FROM", other_mysql)
        other_select = other_select_result.group()
        for word in oname_community_letters:
            other_select = re.sub(word + "\\.sname as " + word + "sname,*", "", other_select)
            other_select = re.sub(word + "\\.oname as " + word + "oname,*", "", other_select)
            other_select = re.sub(word + "\\.sname,*", "", other_select)
            other_select = re.sub(word + "\\.oname,*", "", other_select)
        for number in first_select_number_list:
            for row_num in range(len(number_list)):
                for col_num in range(len(number_list[row_num])):
                    if number_list[row_num][col_num] == number:
                        word = chr(row_num)
                        if col_num == 0:
                            other_select = re.sub(word + "\\.sname (as " + word + "sname)?,?", "", other_select)
                        else:
                            other_select = re.sub(word + "\\.oname (as " + word + "oname)?,?", "", other_select)

        other_mysql = re.sub("SELECT .* FROM", other_select, other_mysql)

        # 在后面填上所有和连接点相关的点的值，用in关键字
        in_values = []
        for point in connection_point:
            for i in range(len(number_list)):
                if i in oname_community:
                    continue
                if number_list[i][0] == point:
                    in_values.append((chr(i + 97), 's'))
                    break
                elif number_list[i][1] == point:
                    in_values.append((chr(i + 97), 'o'))
                    break
        # 此处连接处的格式非常重要，直接影响查询好不好使
        in_string = " AND "
        value_num = 0
        for value in in_values:
            in_string += value[0] + "." + value[1] + "name='{0[" + str(value_num) + "]}' AND "
            value_num += 1
        in_string = in_string.rstrip(' AND ')
        other_mysql += in_string

        sub_mysql = re.sub("\n", " ", sub_mysql)
        sub_mysql = re.sub(", *FROM", " FROM", sub_mysql)
        other_mysql = re.sub("\n", " ", other_mysql)
        other_mysql = re.sub(", *FROM", " FROM", other_mysql)
        other_mysql = re.sub(" +", " ", other_mysql)
        sub_mysql = re.sub(" +", " ", sub_mysql)
        return True, p_list, sub_mysql.replace(';', '') + ";", other_mysql.replace(';', '') + ";", len(connection_point)

    def get_select(self, query):
        select_list = []
        # 提取出select部分
        select_part = re.search("SELECT (.*) FROM", query).group(1)
        # 提取出x.xname形式
        name_pattern = re.compile("([a-z])\\.([a-z])name")
        names = name_pattern.findall(select_part)
        # 提取到where部分
        where_part = re.search("WHERE .*", query).group()
        #找到p
        for name in names:
            name_result = re.search(name[0] + "\\.pname *= *'([^']*)' AND", where_part)
            if name_result:
                select_list.append((name_result.group(1), name[1]))
        return select_list

    def judge_list_equal(self, list_1, list_2):
        if len(list_1) != len(list_2):
            # # print("长度不同，返回False")
            return False

        for val_1 in list_1:
            once_flag = False
            for val_2 in list_2:
                if val_1 == val_2:
                    once_flag = True
            if not once_flag:
                # print("list1中有元素list2中没有，返回False")
                return False

        for val_2 in list_2:
            once_flag = False
            for val_1 in list_1:
                if val_1 == val_2:
                    once_flag = True
            if not once_flag:
                # print("list1中有元素list2中没有，返回False")
                return False
        # print("正常返回true")
        return True

    def rdb_second_query(self, query, parameters):
        db = pymysql.connect(host=self.host, port=int(self.port), user=self.user,
                             passwd=self.password, db=self.db, charset=self.charset)
        cursor = db.cursor()
        start = time.perf_counter()
        # try:
        if len(query) == 0:
            print("other mysql is None, the whole query is a sub query")
            return
        query += ";"
        all_result = []
        wrong_flag = False
        print(parameters[:3])
        if len(parameters) == 0:
            print("first query result is None, skip the second query")
            return
        show_flag = False
        for parameter in parameters:
            final_query = query.format(list(parameter))
            try:
                final_query = final_query.replace(";", "") + ";"
                if not show_flag:
                    print("Second Other Query Final Query: " + final_query)
                    show_flag = True
                cursor.execute(final_query)
                result = cursor.fetchall()
                if len(result) != 0:
                    all_result.append(result)
            except Exception as e:
                if not wrong_flag:
                    print("_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_++_+_+_+_+_")
                    print(e)
                    print("second " + query)
                    print(parameter)
                    print("++_)+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")
                    wrong_flag = True
                    break
        # if len(parameters[0]) == 1:
        #
        # else:
        #     for parameter in parameters:
        #         final_parameter = "("
        #         for para in parameter:
        #             final_parameter += "'" + para + "',"
        #         final_parameter = final_parameter.rstrip(",") + ")"
        #         final_query = query.format(final_parameter)
        #         final_query = final_query.replace(";", "") + ";"
        #         # print("Second Other Query Final Query: " + final_query)
        #         try:
        #             print(final_query)
        #             cursor.execute(final_query)
        #             result = cursor.fetchall()
        #             if len(result) != 0:
        #                 all_result.append(result)
        #         except Exception as e:
        #             if not wrong_flag:
        #                 print("_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_++_+_+_+_+_")
        #                 print(e)
        #                 print("second " + query)
        #                 print(parameter)
        #                 print("++_)+_+_+_+_+_+_+_+_+_+_+_+_+_+_+_+")
        #                 wrong_flag = True
        #                 break

        all_result = list(set(all_result))
        end = time.perf_counter()
        self.query_time = end - start
        print("Final query result length:" + str(len(all_result)))
        db.close()

    def get_random_number(self, random_file_name, query_length):
        try:
            shuffle_numbers = []
            with open(random_file_name, 'r', encoding='utf8') as random_f:
                random_lines = random_f.readlines()
                for line in random_lines:
                    shuffle_numbers.append(int(line.rstrip('\r\n')))
        except FileNotFoundError:
            shuffle_numbers = list(range(0, query_length))
            random.shuffle(shuffle_numbers)
            with open(random_file_name, 'w', encoding='utf8') as f:
                for number in shuffle_numbers:
                    f.write(str(number) + "\r\n")
        return shuffle_numbers

    def get_query_order(self, random_file_name, query_length, flag=0):
        """
        获取query进行的顺序
        :param random_file_name: 存储乱序顺序的文件路径
        :param query_length: query的数量
        :param flag: 0为调参模式，只有10个固定query
                     1为顺序模式，20个query按顺序排列
                     2为乱序模式，20个query乱序排列，且每个数据集的query的排列都不一样，
                        但同时每个数据集的乱序每次都一样
        :return: query进行的顺序
        """
        if flag == 0:
            return [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
        elif flag == 1:
            return list(range(0, query_length))
        elif flag == 2:
            return self.get_random_number(random_file_name, query_length)
        else:
            return None

    def lru_view(self):
        file_name = self.cf.get("total", "sql_query_dir") + self.cf.get("total", "dataset") + "_sql_query.txt"
        random_file_name = self.cf.get("total", "random_query_dir") + self.cf.get("total", "dataset") + "_random.txt"
        f = open(file_name, 'r', encoding='utf8')
        queries = f.readlines()
        batch_length = int(len(queries) / 5)
        print("One Batch Length:" + str(batch_length))
        for line_number_first in range(len(queries)):
            queries[line_number_first] = queries[line_number_first].rstrip('\r\n')
            queries[line_number_first] = queries[line_number_first].rstrip('\n')
        start = time.perf_counter()
        batch_start = time.perf_counter()
        # print("开始运行......................")
        batch_count = 0
        batch_number = 1
        total_start = time.perf_counter()
        query_numbers = self.get_query_order(random_file_name, len(queries), 1)
        for query_number in query_numbers:
            print("=========================================================================")
            ## 1.进入一个query
            print("No." + str(query_number) + " query start:")
            query = queries[query_number]

            ## 2.寻找子结构
            has_sub, sub_p_list, sub_mysql, other_mysql, length = self.find_oname(query)
            print(sub_mysql)
            print(other_mysql)
            print(length)
            p_list = self.get_p(query)
            not_in_dict = False
            for p in set(p_list):
                if p not in self.p_is_in_view:
                    not_in_dict = True
                    break
            if not_in_dict:
                print("A Predicate is NOT IN THE Predicate Dict, choose to query directly.")
            if not has_sub or not_in_dict:  # 3.如果不存在子结构或者存在不存在的p就直接进行query
                print("batch count:" + str(batch_count))
                print("This query doesn't have sub query.")
                print("Now querying by RDB-only...")
                t1 = threading.Thread(target=self.rdb_query, args=(query,))
                t1.start()
                t1.join(500)
                if t1.is_alive():
                    self.stop_thread(t1)
                    print("No." + str(query_number) + " query overtime:500s")
                    self.rdb_total_time += 500.0
                else:
                    print("No." + str(query_number) + " query running successfully:" + str(self.query_time))
                    self.rdb_total_time += self.query_time

                if (batch_count + 1) % batch_length == 0 and batch_count != 0:
                    batch_end = time.perf_counter()
                    print("No." + str(batch_number) + " batch query over, time:" + str(batch_end - batch_start))
                    batch_number += 1
                    batch_start = time.perf_counter()
                batch_count += 1
                continue
            ## 4.比对子查询和视图的p和查询部分比对
            is_viewed = False

            select_list = self.get_select(sub_mysql)
            latest_view_number = 0
            for view in self.view_actual.keys():  # 判断这个query的p是否在已经创建的view中
                # if self.judge_list_equal(sub_p_list, self.views[view]):  # 必须完全相同
                #     # 第二步：判断select部分是否相同
                #     if set(select_list).issubset(set(self.view_select[view])) and set(self.view_select[view]).issubset(select_list):
                #         is_viewed = True
                #         latest_view_number = view
                #         break

                # 原有的判断方法有问题，不能保证输出正确的结果。
                # 采用新的：必须比对两个子结构，完全一致才可以，此外必须保证整个查询的格式是一样的
                if self.view_actual[view] == sub_mysql:
                    is_viewed = True
                    latest_view_number = view
                    break

            # 5.如果全部对上了，用这个视图进行子查询
            if is_viewed:  # 如果已经在视图中，那么就直接进行查询，并更新视图顺序
                new_query = "SELECT * FROM view" + str(latest_view_number)

                print("All the p in the view, choose to use view query: view" + str(latest_view_number))
                print("Now querying by the view...")
                t = threading.Thread(target=self.rdb_view_query, args=(new_query, length))
                t.start()
                t.join(500)
                if t.is_alive():
                    self.stop_thread(t)
                    print("No." + str(query_number) + " query View part overtime:500s")
                    self.rdb_total_time += 500.0
                    print("batch count:" + str(batch_count))
                    if (batch_count + 1) % batch_length == 0 and batch_count != 0:
                        batch_end = time.perf_counter()
                        print("No." + str(batch_number) + " batch query over, time:" + str(batch_end - batch_start))
                        batch_number += 1
                        batch_start = time.perf_counter()
                    batch_count += 1
                    continue
                else:
                    print("No." + str(query_number) + "query viwe part running successfully:" + str(self.query_time))
                    self.rdb_total_time += self.query_time
                # 完成query
                if len(self.one_results) == 0:
                    print("first query result is None, skip the second query")
                    print("batch count:" + str(batch_count))
                    if (batch_count + 1) % batch_length == 0 and batch_count != 0:
                        batch_end = time.perf_counter()
                        print("No." + str(batch_number) + " batch over, time:" + str(batch_end - batch_start))
                        batch_number += 1
                        batch_start = time.perf_counter()
                    batch_count += 1
                    continue
                parameters = self.one_results
                print("Sub query result length:" + str(len(self.one_results)))
                print("Now querying last result by the other query...")
                t1 = threading.Thread(target=self.rdb_second_query, args=(other_mysql, parameters))
                t1.start()
                t1.join(500)
                if t1.is_alive():
                    self.stop_thread(t1)
                    print("No." + str(query_number) + " last result overtime:500s")
                    self.rdb_total_time += 500.0
                else:
                    print("No." + str(query_number) + " last result running successfully, time :" + str(self.query_time))
                    self.rdb_total_time += self.query_time
                self.delete_queue.remove(latest_view_number)
                self.delete_queue.append(latest_view_number)
                breakdown_time = time.perf_counter()
                self.breakdown[query_number] = breakdown_time - total_start

                print("batch count:" + str(batch_count))
                if (batch_count + 1) % batch_length == 0 and batch_count != 0:
                    batch_end = time.perf_counter()
                    print("No." + str(batch_number) + " batch query over, time:" + str(batch_end - batch_start))
                    batch_number += 1
                    batch_start = time.perf_counter()
                batch_count += 1
                continue
            print("Not in any view, judging if the number is over limit. ")
            # 如果不在视图中，那么首先判断创建的view是否已经超过限制
            # 首先获得当前视图的元组数
            view_number = 0
            for p in set(p_list):
                if not self.p_is_in_view[p]:  #
                    view_number += int(self.p_number[p])
                    self.p_is_in_view[p] = True
            self.every_view_number[self.view_asc_number] = view_number
            # print("总数为: " + str(view_number))
            # 判断是否超过限制
            db = pymysql.connect(host=self.host,
                                 port=self.port,
                                 user=self.user,
                                 passwd=self.password,
                                 db=self.db,
                                 charset=self.charset)
            cursor = db.cursor()
            while view_number + self.view_total_number >= self.mysql_total_number * self.threshold \
                    and batch_count != 0:  # 超过限制
                print("the view number is too much, now drop the view")
                # print(self.p_number)
                # print(query_number)
                # print(self.delete_queue)
                if len(self.delete_queue) == 0:
                    break
                cursor.execute("DROP view view" + str(self.delete_queue[0]) + ";")  # 删除view
                for p in self.views[self.delete_queue[0]]:  # p不再被占用
                    self.p_is_in_view[p] = False
                self.views.pop(self.delete_queue[0])  # 将view的编号从所有view的列表中删除
                self.view_actual.pop(self.delete_queue[0])
                self.view_total_number -= self.every_view_number[self.delete_queue[0]]  # 将view总triple减去删除的
                # print("超过限制，将最早使用的view" + str(self.delete_queue[0]) + '删除')
                self.delete_queue.remove(self.delete_queue[0])  # 在队列中移除这个view编号

            # 如果不超过限制，就建立视图
            print("Not over limit, choose to build view.")
            # 加上alias防止Duplicate column name
            result_select = re.search('SELECT .* FROM', sub_mysql)
            if result_select:
                select_string = result_select.group()
                pattern = re.compile("([a-z])\\.([a-z])name")
                result_names = pattern.findall(select_string)
                for name in result_names:
                    select_string = re.sub(name[0] + "\\." + name[1] + "name",
                                           name[0] + "." + name[1] + "name as " + name[0] + name[1] + "name",
                                           select_string)
                sub_mysql = re.sub("SELECT .* FROM", select_string, sub_mysql)

            view_start = time.perf_counter()
            print("Building View")
            try:
                cursor.execute("CREATE view view" + str(self.view_asc_number) + " AS (" + sub_mysql.replace(';', '') + ");")
                print("Build View Number: " + str(self.view_asc_number))
            except Exception as e:
                print(e)
                cursor.execute("drop view view" + str(self.view_asc_number))
                cursor.execute("CREATE view view" + str(self.view_asc_number) + " AS " + sub_mysql.replace(';', '') + ";")
            view_end = time.perf_counter()
            print("Build view successfully, time:" + str(view_end - view_start))
            self.transfer_time += view_end - view_start

            # 进行本次query
            new_query = "SELECT * FROM view" + str(self.view_asc_number) + ";"
            print("Running view part query...")
            first_overtime_flag = False
            t2 = threading.Thread(target=self.rdb_view_query, args=(new_query, length))
            t2.start()
            t2.join(500)
            if t2.is_alive():
                self.stop_thread(t2)
                print("No." + str(query_number) + " query view part overtime:500s")
                self.rdb_total_time += 500.0
                first_overtime_flag = True
            else:
                print("No." + str(query_number) + " Query view part running successfully:" + str(self.query_time))
                self.rdb_total_time += self.query_time
            # except:
            #     print(sub_mysql)
            #     print(other_mysql)
            # 完成query
            if not first_overtime_flag and len(self.one_results) != 0:
                parameters = self.one_results
                print("Sub query result length:" + str(len(self.one_results)))
                print("Other query running...")
                t1 = threading.Thread(target=self.rdb_second_query, args=(other_mysql, parameters))
                t1.start()
                t1.join(500)
                if t1.is_alive():
                    self.stop_thread(t1)
                    print("No." + str(query_number) + " last result query overtime:500s")
                    self.rdb_total_time += 500.0
                else:
                    print("No." + str(query_number) + " last result query running successfully:" + str(self.query_time))
                    self.rdb_total_time += self.query_time
            else:
                print("first query result is None, skip the second query")


            # 不超过限制之后，保持视图
            self.view_select[self.view_asc_number] = self.get_select(sub_mysql)
            self.delete_queue.append(self.view_asc_number)
            self.views[self.view_asc_number] = sub_p_list
            self.view_actual[self.view_asc_number] = sub_mysql
            self.view_total_number += view_number
            print("Built view" + str(self.view_asc_number))
            self.view_asc_number += 1

            breakdown_time = time.perf_counter()
            self.breakdown[query_number] = breakdown_time - total_start

            print("batch count:" + str(batch_count))
            if (batch_count + 1) % batch_length == 0 and batch_count != 0:
                batch_end = time.perf_counter()
                print("No." + str(batch_number) + " batch query over, time:" + str(batch_end - batch_start))
                with open("record127.txt", "a", encoding="utf8") as fw:
                    fw.write("View " + str(batch_number) + "\t" + str(batch_end - batch_start) + "\n")
                batch_number += 1
                batch_start = time.perf_counter()
            batch_count += 1
            cursor.close()
            db.close()

        end = time.perf_counter()
        print("total time:" + str(end - start))
        # print("query时间: " + str(self.rdb_total_time))
        # print("创建view时间:" + str(self.transfer_time))
        # print(self.breakdown)

    def _async_raise(self, tid, exctype):
        """raises the exception, performs cleanup if needed"""
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # """if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def stop_thread(self, thread):
        self._async_raise(thread.ident, SystemExit)


if __name__ == '__main__':
    db = pymysql.connect(host='localhost', port=3306, user='root',
                         passwd='123456', db='lgd', charset='utf8')
    cursor = db.cursor()
    try:
        for i in range(100):
            cursor.execute("drop view view" + str(i))
    except Exception as e:
        pass
    db.close()
    # 0为yago 1为watdiv_L 2为bio2rdf 3为lubm 4为watdiv_S 5为 watdiv_F 6为 watdiv_C
    rdb_test = RDBView()
    rdb_test.lru_view()
