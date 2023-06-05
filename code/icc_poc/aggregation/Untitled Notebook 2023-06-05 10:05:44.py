# Databricks notebook source
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
