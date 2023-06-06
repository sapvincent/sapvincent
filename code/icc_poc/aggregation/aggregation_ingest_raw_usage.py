# Databricks notebook source
# MAGIC %md
# MAGIC Load and aggregate raw usage events in a given month (further breakdown), introduce pipeline per aggregation routine. Examples: 1. Do monthly aggregations per subAccount (SUM,MAX,AVG...), then do monthly aggregation on globalAccountLevel (MAX,SUM,AVG) 2. globalAccount aggregation SUM without intermediate aggregations 3. distinct Users
# MAGIC if CLUS do not provide, parse SLM information to generate mtGlobalConversionRulesMappingV2.0, refer  CC - PAYG Business Models Zhang, Zhaoyu 
# MAGIC based on metric conversion rules, do required intermediate aggregations for each product, original input is raw usage files, output is intermediate level aggregations(e.g. subAccount level)
# MAGIC write intermediate aggregations to MTRA
# MAGIC based on intermediate aggregations, calculate final usage, i.e. SKU blocks in global Account level, take MonthToDate(from month begin to today) as billCycle, do aggregation everyday
# MAGIC write aggregations dataFrame to MTRA data area as delta table

# COMMAND ----------

storage_account_name = "coredatalaketestint"
data_area = "mtra"

# COMMAND ----------

base_location = "abfss://" + data_area + "@" + storage_account_name + ".dfs.core.windows.net"
#inbox_location_rel = "/rawdata/test2/20220927_131540_1B576_batch_0000000.parquet"
inbox_location_rel = "/rawdata/test/usage_1000001254785713.json"
inbox_location = base_location + inbox_location_rel
file_type = "json"

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").option("multiLine", "true").option("compression", "gzip").load(inbox_location)
#df = spark.read.parquet(inbox_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS delta.`/poc/aggregation_usage`

# COMMAND ----------

# 导入Delta库
from delta.tables import *
table_name = "aggregation_usage"

# 定义Delta表路径
delta_table_path_raw_usage = "/poc/" + table_name

# 将DataFrame写入Delta表
df.write.format("delta").mode("overwrite").save(delta_table_path_raw_usage)

# COMMAND ----------

raw_usage_delta = DeltaTable.forPath(spark, "/poc/aggregation_usage")

# 使用Delta API读取数据
df_raw_usage = raw_usage_delta.toDF()

# 显示DataFrame数据
df_raw_usage.show()

