# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run /Workspace/Users/jain.kvikas1983@gmail.com/include

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists mic_vj_training.bronze;
# MAGIC create schema if not exists mic_vj_training.silver;
# MAGIC create schema if not exists mic_vj_training.gold;
