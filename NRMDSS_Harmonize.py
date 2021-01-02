# Databricks notebook source
import pandas as pd
from pyspark.sql import types
from pyspark.sql.functions import col

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

pathArticleMaster="/mnt/adls/TTS/processed/masters/mapping_article_master"
pathPrcNRMDSS="/mnt/adls/TTS/processed/transactions/daily/txn_nrm_dss"

articleMasterDF= spark.read.parquet(pathArticleMaster)
articleMasterDF.createOrReplaceTempView("article_master")

# COMMAND ----------

allPromosDF_T1 = spark.sql ("""
                            select 
                            nrm.*,
                            nvl(value_based_promo_disc,0) + nvl(header_lvl_disc,0) + nvl(free_quantity_value,0) promotion_value
                            from 
                            (select 
                              nrm.country,
                              substr(nrm.time_key,0,4) year_id,
                              substr(nrm.time_key,0,6) month_id,
                              nrm.time_key,
                              nrm.calendar_day,
                              nrm.distributor_code,
                              nrm.site_code,
                              lpad(nrm.outlet_code, 10, '0') outlet_code,
                              nrm.invoice_no,
                              nrm.invoice_category,
                              nrm.billing_item,
                              nrm.product_code,
                              nrm.promotion_id,
                              substr(nrm.promotion_id,1,10) IO,
                              nrm.promotion_mechanism,
                              nrm.promotion_desc,
                              nrm.promo_start_date,
                              nrm.promo_end_date,
                              case when nrm.promotion_type = 'BASKET' and nrm.promotion_mechanism ='BD' then 'BASKET_BD' 
                                   else nrm.promotion_type
                                   end promotion_type,
                              nrm.value_based_promo_disc,
                              nrm.header_lvl_disc,
                              nrm.free_qty_in_cs,
                              nrm.free_qty_in_pc,
                              nrm.free_qty_val_in_cs,
                              nrm.free_qty_val_in_pc,
                              nrm.free_qty_retail_price_pc,
                              nvl(case when nvl(am.PC_per_SU,0) =0 then 0
                                    else (nrm.free_qty_in_cs * am.PC_per_CS * am.price_per_su)/am.PC_per_SU
                                    end,0) +
                              nvl(case when nvl(am.PC_per_SU,0) =0 then 0
                                    else (nrm.free_qty_in_pc * am.price_per_su)/am.PC_per_SU
                                    end,0) as free_quantity_value,
                              case when promotion_mechanism in ('FQ', 'JR', 'NU') then 0 else 1 end on_off_flag
                              from nrm_raw_data nrm
                              left outer join article_master am on nrm.product_code = am.material_code
                            ) nrm
                            """
                         )
allPromosDF_T1.createOrReplaceTempView("all_promos_t1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select promotion_type, promotion_mechanism, sum(promotion_value) promotion_value
# MAGIC from all_promos_t1
# MAGIC group by promotion_type, promotion_mechanism
# MAGIC order by 1,2;

# COMMAND ----------

# WRITE THE HARMONIZED PROMO DATA TO PROCESSED LAYER IN YEAR/MONTH/DAY PARTITION
(
allPromosDF_T1.repartition(2)
              .write
              .partitionBy("year_id", "month_id", "time_key")
              .mode("overwrite")
              .format("parquet")
              .option("compression","snappy")
              .save(pathPrcNRMDSS)
)
