# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py
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
# -


