# Databricks notebook source
spark.conf.set("spark.sql.hive.convertMetastoreParquet", False)

# COMMAND ----------

# MAGIC %md ##### run NRMDSS import cleaning

# COMMAND ----------

# MAGIC %run ./NRMDSS_ImportCleaning

# COMMAND ----------

# MAGIC %md ##### run NRMDSS harmonization

# COMMAND ----------

# MAGIC %run ./NRMDSS_Harmonize

# COMMAND ----------

# cleaning the in mermory dataframes
if (int(len(nrmRawDTRDIS.index)) > 0):
  del nrmRawDTRDIS
if (int(len(nrmRawLINDIS.index)) > 0):
  del nrmRawLINDIS
if (int(len(nrmRawBASKET.index)) > 0):
  del nrmRawBASKET
if (int(len(nrmRawVOLDIS.index)) > 0):
  del nrmRawVOLDIS
if (int(len(nrmRawOTH.index)) > 0):
  del nrmRawOTH

# COMMAND ----------

# MAGIC %md ##### create TEMP table on top of processed NRM data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists edge.processed_nrm_data;
# MAGIC create external table if not exists edge.processed_nrm_data
# MAGIC (
# MAGIC country string,
# MAGIC calendar_day string,
# MAGIC distributor_code string,
# MAGIC site_code string,
# MAGIC outlet_code string,
# MAGIC invoice_no string,
# MAGIC invoice_category string,
# MAGIC billing_item string,
# MAGIC product_code string,
# MAGIC promotion_id string,
# MAGIC IO string,
# MAGIC promotion_mechanism string,
# MAGIC promotion_desc string,
# MAGIC promo_start_date string,
# MAGIC promo_end_date string,
# MAGIC promotion_type string,
# MAGIC value_based_promo_disc double,
# MAGIC header_lvl_disc double,
# MAGIC free_qty_in_cs double,
# MAGIC free_qty_in_pc double,
# MAGIC free_qty_val_in_cs double,
# MAGIC free_qty_val_in_pc double,
# MAGIC free_qty_retail_price_pc double,
# MAGIC free_quantity_value double,
# MAGIC on_off_flag integer,
# MAGIC promotion_value double
# MAGIC )
# MAGIC stored as parquet
# MAGIC partitioned by (year_id int, month_id int, time_key int)
# MAGIC location '/mnt/adls/TTS/processed/transactions/daily/txn_nrm_dss';
# MAGIC 
# MAGIC refresh table edge.processed_nrm_data;
# MAGIC alter table edge.processed_nrm_data recover partitions;
# MAGIC analyze table edge.processed_nrm_data compute statistics;

# COMMAND ----------

# MAGIC %md ##### run post harmonize validations

# COMMAND ----------

# MAGIC %run ./NRMDSS_PostProcessValidation
