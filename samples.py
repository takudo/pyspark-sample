# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# rdd (spark)動作確認
rdd = sc.textFile("sample.txt")
rdd.take(3)

# +
# matplotlib の動作確認

import numpy as np
import matplotlib.pyplot as plt

x = np.arange(-3, 3, 0.1)
y = np.sin(x)
plt.plot(x, y)
# plt.show()

# +
# pandas の動作確認

import pandas as pd

df = pd.read_csv('sample.csv', sep=',')
df.head(2)

# +
# spark の dataframe を pandas で描画

from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)

sdf = sqlContext.createDataFrame(df)
# sdf.show()

sdf.toPandas()
# + {}
# 以下RDDサンプル。参考: https://qiita.com/sotetsuk/items/6e4e2953799078fd6027

# 集計系
rdd = sc.parallelize([1, 2, 3, 5, 8])
rdd.collect() # 全要素
rdd.top(2) # 大きい２つ
rdd.count() # 要素数
rdd.mean()  # 平均
rdd.sum() # 合計
rdd.variance() # 分散
rdd.stdev() # 標準偏差


# +
# 変換系
rdd = sc.parallelize([1, 2, 3, 5, 8])
rdd.map(lambda x: x * 2).collect()
rdd.filter(lambda x: x % 2== 0).collect()
rdd.reduce(lambda x, y: x + y)

rdd = sc.parallelize(["This is a pen", "This is an apple"])
rdd.flatMap(lambda x: x.split()).collect()
# -


