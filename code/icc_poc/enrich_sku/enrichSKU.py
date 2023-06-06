# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Map service.id + service.plan + metric + region to SKU (Material Number)</h1>
# MAGIC parse SLM metadata to mtGlobalServiceToMaterialMappingV2.0, refer CC - PAYG Business Models if CLUS do not commit to provide
# MAGIC write to MTRA data area if CLUS do not provide
# MAGIC for a dataFrame which has service.id + service.plan + metric + region fields, do lookup, fill with SKU field

# COMMAND ----------

# MAGIC %md
# MAGIC this notebook is based on the option if CLUS provide a sku mapping table

# COMMAND ----------

storage_account_name = "coredatalaketestint"
data_area = "mtra"

base_location = "abfss://" + data_area + "@" + storage_account_name + ".dfs.core.windows.net"
inbox_location_rel = "/sandbox/i076186/sku_mapping/sku_mapping.json"
inbox_location = base_location + inbox_location_rel
file_type = "json"

# COMMAND ----------

df = spark.read.format(file_type).option("inferSchema", "true").option("multiLine", "true").option("compression", "gzip").load(inbox_location)
df.show()

# COMMAND ----------

from pyspark.sql.functions import explode

# 将嵌套的数组展开
df_materials = df.select(explode("materials").alias("mara"))
df_mappings = df_materials.select("mara.globalAccount", "mara.environment", "mara.region", "mara.service_id", "mara.service_plan", "mara.sku")

df_mappings.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC load aggregation sum from delta table

# COMMAND ----------

# 导入Delta库
from delta.tables import *
table_name = "aggregation_sum"

# 定义Delta表路径
delta_table_path = "/poc/" + table_name
# 读取Delta表
delta_table = DeltaTable.forPath(spark, delta_table_path)

# 使用Delta API读取数据
df_agg_sum = delta_table.toDF()

# 显示DataFrame数据
df_agg_sum.show(truncate=False)

# COMMAND ----------

# 执行连接操作
joined_df = df_mappings.join(df_agg_sum, on=["globalAccount", "environment", "service_id", "service_plan", "region"], how="inner")

# 显示连接结果
joined_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC save enriched aggregation sum result to delta table /poc/enriched_aggregation_sum

# COMMAND ----------

table_name = "enriched_aggregation_sum"

# 定义Delta表路径
delta_table_path = "/poc/" + table_name

# 将DataFrame写入Delta表
joined_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(delta_table_path)
