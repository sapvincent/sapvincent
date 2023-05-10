# Databricks notebook source
from pyspark.sql import SparkSession

# 创建一个SparkSession对象
spark = SparkSession.builder \
    .appName("Read CSV") \
    .getOrCreate()

# 读取CSV文件并创建DataFrame
df = spark.read.csv("libraries_by_python_version2.csv", header=True, inferSchema=True)

# 显示DataFrame中的数据
df.show()

