# -*- coding: utf-8 -*-
'''
This file is used to train the regression model and get the final result.
'''
import os
import sys
import random
from collections import Counter

from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.svm import SVR
import matplotlib.pyplot as plt

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import re
import shelve
import time

from sklearn.experimental import enable_hist_gradient_boosting
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split, KFold
from sklearn.metrics \
    import explained_variance_score, mean_absolute_error, mean_squared_error, median_absolute_error, r2_score
import numpy as np
import psycopg2

conn = psycopg2.connect(database="watdiv", user="postgres", password="123456", host="localhost", port="5432")
cur = conn.cursor()

all_predicates_types = list()
all_types = list()
source_t0_path = "../res/watdiv/watdiv25_.txt"
train_XY_path = "../res/shelves/watdiv/train_data_S_x1_25_washed"
pca_param = 50
kfold_random_state = 10


def init():
    global all_predicates_types, all_types
    # 获取现有表名
    if False:
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename;")
        table_names = cur.fetchall()
        # 依次删除现有表
        for table_name in table_names:
            cur.execute("drop table {0};".format(table_name[0]))
        # 数据文件路径
        # 创建表t0
        cur.execute("create table t0 (s text null, p text null, o text null)")
        # 将数据文件导入到t0中
        with open(source_t0_path) as f:
            cur.copy_from(f, "t0", sep=" ")
        # 在t0上建立索引
        cur.execute("create index t0_s on t0(s)")
        # 事务更新
        conn.commit()
    cur.execute("select distinct p from t0 order by p")
    all_predicates_types = [x[0] for x in cur.fetchall()]
    cur.execute("select distinct o from t0 where p like '%type%' order by o")
    all_types = [x[0] for x in cur.fetchall()]
    all_predicates_types.extend(all_types)
    all_predicates_types.sort()


ratio_dict = dict()
total_dict = dict()
# with shelve.open("../res/watdiv/watdiv100_distribution") as f:
#     ratio_dict = f["ratio_dict"]
#     total_dict = f["total_dict"]

'''
if there is a xxx_distribution shelve file for loading the two dict, then the following code is unnecessary.
'''
with open(source_t0_path, "r") as f:
    line = f.readline()
    while line:
        l = line.strip().split()
        while len(l) < 3:
            l.append("")
        ratio_dict[(l[1], l[2])] = ratio_dict.setdefault((l[1], l[2]), 0) + 1
        ratio_dict[(l[0], l[1])] = ratio_dict.setdefault((l[0], l[1]), 0) + 1
        ratio_dict[(l[0], l[1], l[2])] = ratio_dict.setdefault((l[0], l[1], l[2]), 0) + 1
        total_dict[l[1]] = total_dict.setdefault(l[1], 0) + 1
        line = f.readline()


def ratio(s_conds, p_conds, o_conds, param):
    # param为目标谓词
    r = 0
    for k, v in p_conds.items():
        for each_v in v:  # 对每一个p谓词（目标谓词）
            if each_v == param and k not in o_conds and k not in s_conds:  # 目标谓词对应的表k，缺少宾语选择
                r += 1  # 即目标谓词无过滤
            elif each_v == param:  # 目标谓词对应的表k，存在宾语选择
                obj = o_conds.get(k)
                sub = s_conds.get(k)
                if param in all_types:  # 如果目标谓词是type类型的宾语
                    if k in s_conds:
                        for each_sub in sub:
                            r += ratio_dict.get(
                                (each_sub, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", param)) / total_dict[
                                     "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", param]
                    else:
                        r += ratio_dict.get(
                            ("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", param)
                        ) / total_dict["<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"]
                else:  # 否则
                    if k in o_conds and k in s_conds:
                        for each_sub in sub:
                            for each_obj in obj:  # 依次考察表k的所有宾语选择
                                if (each_sub, param, each_obj) in ratio_dict:  # 如果存在（目标谓词，宾语）这种组合，则存在过滤
                                    r += ratio_dict.get((each_sub, param, each_obj)) / total_dict[param]
                                else:  # 否则无过滤
                                    r += 0
                    elif k in o_conds:
                        for each_obj in obj:
                            if (param, each_obj) in ratio_dict:
                                r += ratio_dict.get((param, each_obj)) / total_dict[param]
                            else:
                                r += 0
                    elif k in s_conds:
                        for each_sub in sub:
                            if (each_sub, param) in ratio_dict:
                                r += ratio_dict.get((each_sub, param)) / total_dict[param]
                            else:
                                r += 0

    return r


def read_data(filepath):
    with shelve.open(filepath) as f:
        data = f["data"]
    X = list()
    Y = list()
    for storage in data:
        for cost, subquery in data.get(storage).items():  # todo 和cost_execution.py中的训练数据文件格式一样发生了改变
            record = list()
            v_storage = [1 if all_predicates_types[x] in storage else 0 for x in range(len(all_predicates_types))]
            where_clause = re.search("where(.*)", subquery).group(1)
            conditions = where_clause.split(" and ")
            s_conds = dict()
            p_conds = dict()
            o_conds = dict()
            for cond in conditions:
                s_match = re.match("(.*?)\\.s\\s*=\\s*'(.*?)'", cond.strip())
                p_match = re.match("(.*?)\\.p\\s*=\\s*'(.*?)'", cond.strip())
                o_match = re.match("(.*?)\\.o\\s*=\\s*'(.*?)'", cond.strip())
                if s_match:
                    s_conds.setdefault(s_match.group(1), list()).append(s_match.group(2))
                elif p_match:
                    p_conds.setdefault(p_match.group(1), list()).append(p_match.group(2))
                elif o_match:
                    o_conds.setdefault(o_match.group(1), list()).append(o_match.group(2))
            v_query = list()
            for i in range(len(all_predicates_types)):
                # epsilon = 4
                r = ratio(s_conds, p_conds, o_conds, all_predicates_types[i])
                v_query.append(r)
            record.extend(v_storage)
            record.extend(v_query)
            X.append(record)
            Y.append(cost)
    return X, Y


if __name__ == '__main__':
    # train_X, train_Y = read_data("../res/shelves/watdiv/train_data_1_0")
    # print("train data generated")

    print("initialized")
    init()
    train_X, train_Y = read_data(train_XY_path)
    with shelve.open(train_XY_path) as f:
        f["X"] = train_X
        f["Y"] = train_Y
    print("data generated")
    print("train_X shape: {0}x{1}".format(len(train_X), len(train_X[0])))
    dim_before_pca = len(train_X[0])
    print("train_Y shape: {0}".format(len(train_Y)))

    # train_X, test_X, train_Y, test_Y = train_test_split(train_X, train_Y, random_state=10, test_size=0.2)
    kfold = KFold(10, True, random_state=kfold_random_state)
    data = np.hstack((train_X, np.array(train_Y).reshape((len(train_Y), 1))))
    T = np.zeros(kfold.n_splits)  # 指标T，模型回归时间
    Treal = np.zeros(kfold.n_splits)  # 指标T_real，真实时间
    Treal_ = np.zeros(kfold.n_splits)  # 指标T_real',记录的是压缩后时间的T_real
    R2_score_avg = np.zeros(kfold.n_splits)
    mae_avg = np.zeros(kfold.n_splits)
    mape_statistic = list()
    mse_avg = np.zeros(kfold.n_splits)
    mape_avg = np.zeros(kfold.n_splits)
    my_metric_avg = np.zeros(kfold.n_splits)
    train_number = np.zeros(kfold.n_splits)
    test_number = np.zeros(kfold.n_splits)
    k_i = 1  # kfold迭代变量，即第k_i轮
    n_clusters = 1  # 聚类数量（已设置为1）
    for train_indices, test_indices in kfold.split(data):
        T_i = 0  # 指标T
        Treal_i = 0  # 指标T_real
        Treal__i = 0  # 指标T_real',记录的是压缩后时间的T_real
        print("kfold {0}".format(str(k_i)))

        X = data[:][:, :-1]
        Y = data[:][:, -1]
        pca = PCA(pca_param, random_state=10)
        X = pca.fit_transform(X)
        train_X = X[train_indices]
        test_X = X[test_indices]
        train_Y = Y[train_indices]
        test_Y = Y[test_indices]
        # train_X = data[train_indices][:, :-1]
        # train_Y = data[train_indices][:, -1]
        # test_X = data[test_indices][:, :-1]
        # test_Y = data[test_indices][:, -1]
        for ty in test_Y:  # 真实时间统计
            Treal_i += ty
        # ================================
        print("data splitted")
        # 对标签时间进行压缩
        # a = 1
        # train_Y = [a * np.log(train_Y[i] + 1) for i in range(len(train_Y))]
        # test_Y = [a * np.log(test_Y[i] + 1) for i in range(len(test_Y))]
        for ty in test_Y:  # 压缩后的真实时间统计
            Treal__i += ty
        # with shelve.open(train_XY_path) as f:
        #     f["compressed_Y"] = train_Y
        # print("labels compressed")
        model = HistGradientBoostingRegressor(
            max_iter=1000,
            learning_rate=0.1,
            loss='least_squares',
            # max_leaf_nodes=51,
            # min_samples_leaf=20,
            # l2_regularization=200,
            verbose=0
        )
        model.fit(train_X, train_Y)
        print("models fit")
        print("train_X length: ", len(train_X))
        print("test_X length: ", len(test_X))
        print("train MIN:{0} MAX:{1}".format(str(min(train_Y)), str(max(train_Y))))
        print("test MIN:{0} MAX:{1}".format(str(min(test_Y)), str(max(test_Y))))

        start = time.time()
        pred_Y = model.predict(test_X)
        end = time.time()
        T_i += end - start
        R2_score_avg[k_i - 1] = r2_score(test_Y, pred_Y)
        mae_avg[k_i - 1] = mean_absolute_error(test_Y, pred_Y)
        mse_avg[k_i - 1] = mean_squared_error(test_Y, pred_Y)
        mape_avg[k_i - 1] = np.mean(np.abs((pred_Y - test_Y) / test_Y)) * 100
        my_metric_avg[k_i - 1] = 0
        temp_min = 0
        temp_min_i = 0
        for y_i in range(len(test_Y)):
            if test_Y[y_i] == 0:
                continue
            if np.abs(test_Y[y_i] - pred_Y[y_i]) / test_Y[y_i] > temp_min:
                temp_min = np.abs(test_Y[y_i] - pred_Y[y_i]) / test_Y[y_i]
                temp_min_i = y_i
            my_metric_avg[k_i - 1] += np.abs(test_Y[y_i] - pred_Y[y_i]) / test_Y[y_i]
        my_metric_avg[k_i - 1] /= len(test_Y)
        print("My metric: ", my_metric_avg[k_i - 1])
        print(test_Y[temp_min_i], pred_Y[temp_min_i])

        temp_list = list()
        for y_i in range(len(test_Y)):
            temp_list.append(np.abs(test_Y[y_i] - pred_Y[y_i]) / test_Y[y_i])
        mape_statistic.append(np.array(temp_list))

        train_number[k_i - 1] = len(train_X)
        test_number[k_i - 1] = len(test_X)
        print("R2 score: %f -- timing: %f" % (r2_score(test_Y, pred_Y), end - start))
        T[k_i - 1] = T_i
        Treal[k_i - 1] = Treal_i
        Treal_[k_i - 1] = Treal__i
        k_i += 1

    mae_avg = np.delete(mae_avg, [np.argmax(mae_avg), np.argmin(mae_avg)])  # 去掉MAE最大最小
    mse_avg = np.delete(mse_avg, [np.argmax(mse_avg), np.argmin(mse_avg)])  # 去掉MAE最大最小

    mape_avg = np.delete(mape_avg, [np.argmax(mape_avg), np.argmin(mape_avg)])  # 去掉MAPE最大最小
    R2_score_avg = np.delete(R2_score_avg, [np.argmax(R2_score_avg), np.argmin(R2_score_avg)])  # 去掉R2最大最小
    my_metric_avg = np.delete(my_metric_avg, [np.argmax(my_metric_avg), np.argmin(my_metric_avg)])
    # print("T", T)
    # print("Treal", Treal)
    # print("Treal_", Treal_)
    print("R2_score_avg", R2_score_avg)
    #
    # print("train number", train_number)
    # print("test number", test_number)

    print("=" * 50)
    print("AVERAGE RESULT:")
    print("T", np.average(T))
    print("Treal", np.average(Treal))
    print("Treal_", np.average(Treal_))
    print("R2_score_avg:", np.average(R2_score_avg[:]))
    print("MAE:", np.average(mae_avg[:]))
    print("MSE:", np.average(mse_avg[:]))
    print("MAPE:", np.average(mape_avg[:]))
    print("MY metric:", np.average(my_metric_avg[:]))
    print("train number:", np.average(train_number[:]))
    print("test number:", np.average(test_number[:]))
    print("dim before pca:", dim_before_pca)
    print("dimension:", len(train_X[0]))
    print("Average of test label:", np.average(test_Y))
    print("MAPE statistics. 每轮测试数据中的MAPE四分位数，:")
    for i in range(kfold.n_splits):
        temp_mape_list = [0, 0, 0, 0]
        for x in mape_statistic[i]:
            for bin_i in range(4):
                if 0.25 * bin_i <= x < 0.25 * (bin_i + 1):
                    temp_mape_list[bin_i] += 1
        print("kfold {0}".format(i + 1), temp_mape_list)
    print(train_XY_path)
