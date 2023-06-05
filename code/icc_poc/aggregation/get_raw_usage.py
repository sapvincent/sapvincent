# Databricks notebook source
storage_account_name = "coredatalaketestint"
data_area = "mtra"

# COMMAND ----------

base_location = "abfss://" + data_area + "@" + storage_account_name + ".dfs.core.windows.net"
inbox_location_rel = "/rawdata/test2/20220927_131540_1B576_batch_0000000.parquet"
inbox_location = base_location + inbox_location_rel
#usage_location = "https://coredatalaketestint.blob.core.windows.net/mtra/rawdata/test2/20220927_131540_1B576_batch_0000000.parquet"
file_type = "delta"

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").option("multiLine", "true").option("compression", "gzip").load(inbox_location)
