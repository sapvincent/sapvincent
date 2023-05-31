# Databricks notebook source
storage_account_name = "coredatalakesandbox"
data_area = "sbox"

# COMMAND ----------

base_location = "abfss://" + data_area + "@" + storage_account_name + ".dfs.core.windows.net"
inbox_location_rel = "/i076186/dailyAPICallsWithInvalidAirkeyByProduct.json"
inbox_location = base_location + inbox_location_rel
file_type = "json"

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").option("multiLine", "true").option("compression", "gzip").load(inbox_location)

#df = spark.read.json(inbox_location)

# COMMAND ----------

display(df.count())

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# 将 Spark DataFrame 转换为 Pandas DataFrame
pandas_df = df.select("timestamp", "value").toPandas()

# 创建线型图
plt.plot(pandas_df["timestamp"], pandas_df["value"])

# 添加标题和坐标轴标签
plt.title("Daily API Calls With Invalid Airkey")
plt.xlabel("Timestamp")
plt.ylabel("Value")

# 显示图形
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# 计算每个 product 的 sum 值
sum_df = df.groupBy("product").sum("value").toPandas()

# 创建饼图
plt.pie(sum_df["sum(value)"], labels=sum_df["product"], autopct='%1.1f%%')

# 添加标题
plt.title("Product Value Distribution")

# 显示图形
plt.show()

