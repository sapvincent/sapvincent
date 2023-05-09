# This is a sample Python script.
import spark as spark

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


diamonds = (spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
)

diamonds.write.format("delta").save("/mnt/delta/diamonds1")