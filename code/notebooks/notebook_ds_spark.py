# Databricks notebook source
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import sys
venv_dir = sys.prefix
spark_jar_path = venv_dir + "/share/postgresql/postgresql-42.6.0.jar"
# 创建一个SparkSession对象
spark = SparkSession.builder \
    .appName("Read with Spark") \
    .config("spark.jars", spark_jar_path) \
    .getOrCreate()

# 读取CSV文件并创建DataFrame
# df = spark.read.csv("libraries_by_python_version2.csv", header=True, inferSchema=True)

# 显示DataFrame中的数据
# df.show()


# 定义PostgreSQL连接属性
url = "jdbc:postgresql://localhost:5433/postgres"
properties = {
    "user": "postgres",
    "driver": "org.postgresql.Driver"
}

# 读取PostgreSQL数据
df = spark.read.jdbc(url=url, table="dailyhttpsummaryfromsplunk", properties=properties)

# 显示数据
# df.show()

# 将 DataFrame 转换为 pandas DataFrame
pandas_df = df.toPandas()

# 绘制时间序列图
plt.plot(pandas_df['date_column'], pandas_df['count_column'])

# 设置图表标题和轴标签
plt.title("Time Series for C4C")
plt.xlabel("Date")
plt.ylabel("Count")

# 自动旋转日期标签
plt.gcf().autofmt_xdate()

# 显示图表
plt.show()
