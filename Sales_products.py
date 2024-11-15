# Databricks notebook source
# MAGIC %run /Workspace/Users/jain.kvikas1983@gmail.com/include

# COMMAND ----------

df=spark.table("mic_vj_training.bronze.sales")
df1=df.dropDuplicates().dropna().select("order_id","customer_id","transaction_id","product_id","quantity","discount_amount","order_date","total_amount")
df1.write.mode("overwrite").saveAsTable("mic_vj_training.silver.sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view products_bronze as (select * from mic_vj_training.bronze.products)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if NOT EXISTS mic_vj_training.silver.products (
# MAGIC   product_id INT,
# MAGIC   product_name STRING,
# MAGIC   product_category STRING,
# MAGIC   product_price DOUBLE
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deduplicated_bp AS (
# MAGIC   SELECT
# MAGIC     bp.*,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY seqNum
# MAGIC DESC) AS row_num
# MAGIC   FROM
# MAGIC     products_bronze bp
# MAGIC )
# MAGIC MERGE INTO mic_vj_training.silver.products sp
# MAGIC USING (
# MAGIC   SELECT * FROM deduplicated_bp WHERE row_num = 1
# MAGIC ) bp
# MAGIC ON sp.product_id = bp.product_id 
# MAGIC WHEN MATCHED AND bp.operation = 'UPDATE'
# MAGIC THEN
# MAGIC   UPDATE SET
# MAGIC     product_name = bp.product_name,
# MAGIC     product_category = bp.product_category,
# MAGIC     product_price = bp.product_price
# MAGIC WHEN MATCHED AND bp.operation = 'DELETE'
# MAGIC THEN
# MAGIC   DELETE  
# MAGIC WHEN NOT MATCHED
# MAGIC THEN
# MAGIC   INSERT (
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     product_category,
# MAGIC     product_price
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     bp.product_id,
# MAGIC     bp.product_name,
# MAGIC     bp.product_category,
# MAGIC     bp.product_price
# MAGIC   )
