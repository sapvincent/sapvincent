# Databricks notebook source
# MAGIC %md
# MAGIC read data from aggregation_usage

# COMMAND ----------

# import delta lib
from delta.tables import *
raw_usage_delta = DeltaTable.forPath(spark, "/poc/aggregation_usage")

# convert delta table to dataframe
df_raw_usage = raw_usage_delta.toDF()

# show dataframe
df_raw_usage.show()

# COMMAND ----------

from pyspark.sql.functions import explode
from pyspark.sql.functions import sum

# explode array node
df_usage = df_raw_usage.select(explode("usage").alias("usage_exploded"))
df_consumer = df_usage.selectExpr("usage_exploded.consumer.globalAccount", "usage_exploded.consumer.subAccount", "usage_exploded.consumer.environment", "usage_exploded.consumer.instance", "usage_exploded.consumer.region", "usage_exploded.service.id as service_id", "usage_exploded.service.plan as service_plan", "explode(usage_exploded.measures.value) as sum_value")
df_measures = df_consumer.groupBy("globalAccount","environment","service_id","region", "service_plan").agg(sum("sum_value").alias("sum_value"))

df_measures.show()
display(df_measures.count())

# COMMAND ----------

# MAGIC %md
# MAGIC save aggregation sum data to delta table

# COMMAND ----------

# import delta table lib
from delta.tables import *
table_name = "aggregation_sum"

# define delta table path
delta_table_path = "/poc/" + table_name

# write datafram into delta table
df_measures.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(delta_table_path)

# COMMAND ----------

# read delta table
delta_table = DeltaTable.forPath(spark, "/poc/aggregation_sum")

# convert delta table into dataframe
df = delta_table.toDF()

# show dataframe
df.show(truncate=False)
