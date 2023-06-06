# Databricks notebook source
# MAGIC %md
# MAGIC Rating
# MAGIC load billCycleGlobalAccountLevelUsage into a dataFrame
# MAGIC The loaded dataFrame has SKU and quantity, join pricing for SKU (mtGlobalServiceConfigV2.0 and mtGlobalServicePriceV2.0) and calculate amount for each row, amount = quantity * unitPrice
# MAGIC write to MTRA data area as delta table
