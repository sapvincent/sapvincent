# Databricks notebook source
# MAGIC %md
# MAGIC read data from aggregation_usage

# COMMAND ----------

# 导入Delta库
from delta.tables import *
raw_usage_delta = DeltaTable.forPath(spark, "/poc/aggregation_usage")

# 使用Delta API读取数据
df_raw_usage = raw_usage_delta.toDF()

# 显示DataFrame数据
df_raw_usage.show()

# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql.functions import sum

# 将嵌套的数组展开
df_usage = df_raw_usage.select(explode("usage").alias("usage_exploded"))
df_consumer = df_usage.selectExpr("usage_exploded.consumer.globalAccount", "usage_exploded.consumer.subAccount", "usage_exploded.consumer.environment", "usage_exploded.consumer.instance", "usage_exploded.consumer.region", "usage_exploded.service.id as service_id", "usage_exploded.service.plan as service_plan", "explode(usage_exploded.measures.value) as sum_value")
df_measures = df_consumer.groupBy("globalAccount","environment","service_id","region", "service_plan").agg(sum("sum_value").alias("sum_value"))

df_measures.show()
display(df_measures.count())

# COMMAND ----------

# MAGIC %md
# MAGIC save aggregation sum data to delta table

# COMMAND ----------

# 导入Delta库
from delta.tables import *
table_name = "aggregation_sum"

# 定义Delta表路径
delta_table_path = "/poc/" + table_name

# 将DataFrame写入Delta表
df_measures.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(delta_table_path)

# COMMAND ----------

# 读取Delta表
delta_table = DeltaTable.forPath(spark, "/poc/aggregation_sum")

# 使用Delta API读取数据
df = delta_table.toDF()

# 显示DataFrame数据
df.show(truncate=False)
