# -*- coding: utf-8 -*-
'''
This file is used to show some statistics about train data.
'''
import shelve
import matplotlib.pyplot as plt
from collections import Counter

train_XY_path = "../res/shelves/lubm/train_data_200w_x10_washed"
# train_XY_path = "../res/shelves/watdiv/train_data_F_x5_100_washed"
with shelve.open(train_XY_path) as f:
    train_X = f["X"]
    train_Y = f["Y"]
# train_Y = train_Y[:100]
n = 0
train_Y_ = []
for i, y in enumerate(train_Y):
    if 10 + n * 140 <= i <= 19 + n * 140:
        train_Y_.append(y)
    n = len(train_Y_) // 10
train_Y_ = train_Y_[:200]
X = [i for i in range(len(train_Y_))]
plt.figure(figsize=(10, 5))
plt.scatter(x=X, y=train_Y_, marker='.', alpha=0.5)
plt.show()
y_ = list()
for y in train_Y:
    y_.append(int(y))
y_.sort()
counter = Counter(y_)
print(counter)
# plt.hist(x=train_Y)
# plt.show()
