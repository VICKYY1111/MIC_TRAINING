# Databricks notebook source
# MAGIC %run /Workspace/Users/jain.kvikas1983@gmail.com/include

# COMMAND ----------

# DBTITLE 1,PySpark
df=spark.table("mic_vj_training.bronze.customers")
df1=df.dropDuplicates().dropna()
df1.write.mode("overwrite").saveAsTable("mic_vj_training.silver.customers")

# COMMAND ----------

# DBTITLE 1,Spark SQL
# MAGIC %sql
# MAGIC -- create or replace table michelin.silver.sales as (select distinct order_id, customer_id,transaction_id,product_id,quantity,discount_amount,total_amount,order_date from michelin.bronze.sales where order_id is not null)
# MAGIC SHOW CREATE TABLE mic_vj_training.bronze.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view customers_bronze as (select * from mic_vj_training.bronze.customers)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS mic_vj_training.silver.customers (
# MAGIC   customer_id INT,
# MAGIC   customer_name STRING,
# MAGIC   customer_email STRING,
# MAGIC   customer_city STRING,
# MAGIC   customer_state STRING,
# MAGIC   operation STRING,
# MAGIC   sequenceNum INT,
# MAGIC   ingestion_date TIMESTAMP,
# MAGIC   source_path STRING)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deduplicated_bp AS (
# MAGIC   SELECT
# MAGIC     cb.*,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY sequenceNum DESC) AS row_num
# MAGIC   FROM
# MAGIC     customers_bronze cb
# MAGIC )
# MAGIC MERGE INTO mic_vj_training.silver.customers sc
# MAGIC USING (
# MAGIC   SELECT * FROM deduplicated_bp WHERE row_num = 1
# MAGIC ) dbp
# MAGIC ON sc.customer_id = dbp.customer_id 
# MAGIC WHEN MATCHED AND dbp.operation = 'UPDATE'
# MAGIC THEN
# MAGIC   UPDATE SET
# MAGIC     customer_name = dbp.customer_name,
# MAGIC     customer_email = dbp.customer_email,
# MAGIC     customer_city = dbp.customer_city,
# MAGIC     customer_state = dbp.customer_state,
# MAGIC     operation = dbp.operation,
# MAGIC     sequenceNum = dbp.sequenceNum,
# MAGIC     ingestion_date = dbp.ingestion_date,
# MAGIC     source_path = dbp.source_path
# MAGIC WHEN MATCHED AND dbp.operation = 'DELETE'
# MAGIC THEN
# MAGIC   DELETE  
# MAGIC WHEN NOT MATCHED
# MAGIC THEN
# MAGIC   INSERT (
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     customer_email,
# MAGIC     customer_city,
# MAGIC     customer_state,
# MAGIC     operation,
# MAGIC     sequenceNum,
# MAGIC     ingestion_date,
# MAGIC     source_path
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     dbp.customer_id,
# MAGIC     dbp.customer_name,
# MAGIC     dbp.customer_email,
# MAGIC     dbp.customer_city,
# MAGIC     dbp.customer_state,
# MAGIC     dbp.operation,
# MAGIC     dbp.sequenceNum,
# MAGIC     dbp.ingestion_date,
# MAGIC     dbp.source_path
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH source_cte AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     customer_email,
# MAGIC     customer_city,
# MAGIC     customer_state,
# MAGIC     operation,
# MAGIC     sequenceNum,
# MAGIC     ingestion_date,
# MAGIC     source_path,
# MAGIC     current_timestamp() AS start_date,
# MAGIC     NULL AS end_date,
# MAGIC     TRUE AS is_current,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY customer_id 
# MAGIC       ORDER BY sequenceNum DESC, ingestion_date DESC
# MAGIC     ) AS rn
# MAGIC   FROM mic_vj_training.bronze.customers
# MAGIC )
# MAGIC MERGE INTO mic_vj_training.silver.customers AS target
# MAGIC USING (
# MAGIC   SELECT *
# MAGIC   FROM source_cte
# MAGIC   WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id AND target.is_current = TRUE
# MAGIC WHEN MATCHED AND source.operation = 'UPDATE' THEN
# MAGIC   UPDATE SET
# MAGIC     target.end_date = current_timestamp(),
# MAGIC     target.is_current = FALSE
# MAGIC
# MAGIC WHEN MATCHED and source.operation='DELETE' THEN
# MAGIC  UPDATE SET
# MAGIC     target.end_date = current_timestamp(),
# MAGIC     target.is_current = FALSE
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     customer_email,
# MAGIC     customer_city,
# MAGIC     customer_state,
# MAGIC     operation,
# MAGIC     sequenceNum,
# MAGIC     ingestion_date,
# MAGIC     source_path,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     is_current
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.customer_id,
# MAGIC     source.customer_name,
# MAGIC     source.customer_email,
# MAGIC     source.customer_city,
# MAGIC     source.customer_state,
# MAGIC     source.operation,
# MAGIC     source.sequenceNum,
# MAGIC     source.ingestion_date,
# MAGIC     source.source_path,
# MAGIC     source.start_date,
# MAGIC     source.end_date,
# MAGIC     source.is_current
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view mic_vj_training.silver.customer_current as 
# MAGIC select customer_id,customer_name, customer_email,customer_city, customer_state from mic_vj_training.silver.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table mic_vj_training.silver.sales_customer
# MAGIC (select s.customer_id,s.product_id,s.quantity,s.discount_amount, s.total_amount,c.customer_name,c.customer_city,c.customer_state
# MAGIC from mic_vj_training.silver.sales s
# MAGIC inner join mic_vj_training.silver.customer_current c
# MAGIC on s.customer_id=c.customer_id)
