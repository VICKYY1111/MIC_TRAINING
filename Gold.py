# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace view mic_vj_training.gold.customer_total_sales as (select customer_id,customer_name, round(sum(total_amount)) as total_amount from mic_vj_training.silver.sales_customer group by all )

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view mic_vj_training.gold.total_sale as (select round(sum(total_amount)) as total_sales from mic_vj_training.silver.sales)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from mic_vj_training.gold.customer_total_sales
