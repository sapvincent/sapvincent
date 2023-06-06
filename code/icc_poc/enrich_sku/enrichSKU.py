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

# explode array node
df_materials = df.select(explode("materials").alias("mara"))
df_mappings = df_materials.select("mara.globalAccount", "mara.environment", "mara.region", "mara.service_id", "mara.service_plan", "mara.sku")

df_mappings.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC load aggregation sum from delta table

# COMMAND ----------

# import delta table lib
from delta.tables import *
table_name = "aggregation_sum"

# define delta table path
delta_table_path = "/poc/" + table_name
# read data table
delta_table = DeltaTable.forPath(spark, delta_table_path)

# convert delta table to dataframe
df_agg_sum = delta_table.toDF()

# show dataframe
df_agg_sum.show(truncate=False)

# COMMAND ----------

# join mapping table with aggregated result, so that the sku can be enriched to the aggregation result
joined_df = df_mappings.join(df_agg_sum, on=["globalAccount", "environment", "service_id", "service_plan", "region"], how="inner")

# show result
joined_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC save enriched aggregation sum result to delta table /poc/enriched_aggregation_sum

# COMMAND ----------

table_name = "enriched_aggregation_sum"

# define delta table path
delta_table_path = "/poc/" + table_name

# write dataframe into delta table
joined_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(delta_table_path)
