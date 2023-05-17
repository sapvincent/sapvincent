# Databricks notebook source
from delta.tables import DeltaTable

# Create table in the metastore
DeltaTable.createIfNotExists(spark) \
  .tableName("default.people10m") \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .execute()

# Create or replace table with path and add properties
DeltaTable.createOrReplace(spark) \
  .addColumn("id", "INT") \
  .addColumn("firstName", "STRING") \
  .addColumn("middleName", "STRING") \
  .addColumn("lastName", "STRING", comment = "surname") \
  .addColumn("gender", "STRING") \
  .addColumn("birthDate", "TIMESTAMP") \
  .addColumn("ssn", "STRING") \
  .addColumn("salary", "INT") \
  .property("description", "table with people data") \
  .location("/tmp/delta/people10m") \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW people_updates (
# MAGIC   id, firstName, middleName, lastName, gender, birthDate, ssn, salary
# MAGIC ) AS VALUES
# MAGIC   (9999998, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 55250),
# MAGIC   (9999999, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500),
# MAGIC   (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000),
# MAGIC   (20000001, 'John', '', 'Doe', 'M', '1978-01-14T04:00:00.000+000', '345-67-8901', 55500),
# MAGIC   (20000002, 'Mary', '', 'Smith', 'F', '1982-10-29T01:00:00.000+000', '456-78-9012', 98250),
# MAGIC   (20000003, 'Jane', '', 'Doe', 'F', '1981-06-25T04:00:00.000+000', '567-89-0123', 89900);
# MAGIC
# MAGIC MERGE INTO people_10m
# MAGIC USING people_updates
# MAGIC ON people_10m.id = people_updates.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

table_name = "default.people10m"
people_df = spark.read.table(table_name)

display(people_df)
