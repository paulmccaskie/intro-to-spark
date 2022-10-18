# Databricks notebook source
# This is a sample

# COMMAND ----------

diamondsDf = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")


# COMMAND ----------

display(diamondsDf)

# COMMAND ----------

diamondsDf.write.format("delta").mode("overwrite").save("/delta/diamonds")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS diamonds;
# MAGIC 
# MAGIC CREATE TABLE diamonds USING DELTA LOCATION '/delta/diamonds/'

# COMMAND ----------

rowCount = diamondsDf.count()
print(f"number of rows: {rowCount}")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * 
# MAGIC from diamonds

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * 
# MAGIC from diamonds
# MAGIC where cut = 'Premium'

# COMMAND ----------

queryDf = spark.sql("SELECT * FROM diamonds")
display(queryDf)

# COMMAND ----------

premiumDiamondsDf = diamondsDf.filter("cut = 'Premium'")

# COMMAND ----------

display(premiumDiamondsDf)

# COMMAND ----------

premiumDiamondsDf.write.format("delta").mode("overwrite").save("/delta/things")

# COMMAND ----------

from pyspark.sql.functions import avg

display(diamondsDf.select("color","price").groupBy("color").agg(avg("price")).sort("color"))

# COMMAND ----------


