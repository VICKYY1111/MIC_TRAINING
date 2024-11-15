# Databricks notebook source
#dbutils.fs.mount(
  #source = "wasbs://data@adlstraining123.blob.core.windows.net",
  #mount_point = "/mnt/adlstraining123/data",
  #extra_configs = {"fs.azure.account.key.adlstraining123.blob.core.windows.net":"key"})

# COMMAND ----------

# MAGIC %fs ls "dbfs:/mnt/adlstraining123/data"

# COMMAND ----------

# MAGIC %run /Workspace/Users/jain.kvikas1983@gmail.com/include

# COMMAND ----------

input_path

# COMMAND ----------

dbutils.widgets.text("catalog","mic_vj_training")
catalog=dbutils.widgets.get("catalog")

# COMMAND ----------

dbutils.widgets.text("schema","bronze")
schema=dbutils.widgets.get("schema")

# COMMAND ----------

dbutils.widgets.text("table","")
source_file_name=dbutils.widgets.get("table")

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}sales.csv",header=True,inferSchema=True)
df_sales_final=add_ingestion_col(df_sales)
#df_final.write.mode("overwrite").saveAsTable(f"{schema}.{source_file_name}")

# COMMAND ----------

dbutils.widgets.text("source","")
source_file_name=dbutils.widgets.get("source")
 

# COMMAND ----------

df=spark.read.csv(f"{input_path}{source_file_name}.csv",header=True,inferSchema=True)
df_final=add_ingestion_col(df)
df_final.write.mode("overwrite").saveAsTable(f"bronze.{source_file_name}")
