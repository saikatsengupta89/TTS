# Databricks notebook source
# MAGIC %md ##### READ BDL DELTA

# COMMAND ----------

from pyspark.sql.functions import *
spark.conf.set("spark.databricks.io.cache.enabled", "true")

# COMMAND ----------

# OLD HISTORICAL LOAD STRATEGY

# stgPath = "/mnt/adls/EDGE_Analytics/Datalake/Staging/Historical/Transaction/DSS/2020/11/11/YYYY=2020"
# stgPath2 = "/mnt/adls/EDGE_Analytics/Datalake/Staging/Historical/Transaction/DSS/2020/11/11/YYYY=2018"

# dataset2018 = spark.read.parquet(stgPath2)
# dataset2018.createOrReplaceTempView("stg_dss_2018")

# %sql
# select 
# /*+ REPARTITION(50) */
# concat(date_format(InvoiceDate,"yyyy"), date_format(InvoiceDate, "MM")) month_key, 
# count(1) total_cnt
# from stg_dss_2018 where CountryCode="VN"
# group by concat(date_format(InvoiceDate,"yyyy"), date_format(InvoiceDate, "MM"));

# dataDSSPartitoned.repartition(10) \
#                  .write \
#                  .partitionBy("year_id", "month_id", "time_key") \
#                  .mode("append") \
#                  .format("parquet") \
#                  .option("compression","snappy") \
#                  .save(procPath)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/adls/BDL_Gen2/BusinessDataLake/CD/SecSales/Global/Online_Countries/Transactional/OnlineInvoiceSKUSales/"))

# COMMAND ----------

dssProcPath = "/mnt/adls/EDGE_Analytics/Datalake/Processed/transactions/daily/txn_daily_sales"
bdlDeltaPath="/mnt/adls/BDL_Gen2/BusinessDataLake/CD/SecSales/Global/Online_Countries/Transactional/OnlineInvoiceSKUSales/OnlineInvoiceSKUSales_hist_parquet/"
bdlDeltaPathIncr="/mnt/adls/BDL_Gen2/BusinessDataLake/CD/SecSales/Global/Online_Countries/Transactional/OnlineInvoiceSKUSales/OnlineInvoiceSKUSales_parquet/"

# COMMAND ----------

dbutils.fs.rm("/mnt/adls/EDGE_Analytics/Datalake/Processed/transactions/daily/txn_daily_sales",True)

# COMMAND ----------

dssBdlDFHist = spark.read \
                    .format("delta") \
                    .load(bdlDeltaPath)
dssBdlDFHist.createOrReplaceTempView("online_invoice_sku_sales")

dssBdlDF= spark.read \
               .format("delta") \
               .load(bdlDeltaPathIncr)
dssBdlDF.createOrReplaceTempView("online_invoice_sku_sales_incr")

# COMMAND ----------

dataDSSPartitoned=spark.sql("select " +
                            "CountryCode as country_code, "+
                            "substring(ArticleCode, -8, 8) as product_code, "+
                            "InvoiceNumber as invoice_number, "+
                            "concat(SiteCode,'_',date_format(InvoiceDate, 'yyyy')) site_code_year, "+
                            "DistributorCode as transactional_distributor_code, "+
                            "SiteCode as transactional_site_code, "+
                            "OutletCode as transactional_outlet_code, "+
                            "InvoiceDate as invoice_date, "+
#                           below three custom fields are created for partitioning in the data lake
                            "cast(date_format(InvoiceDate,'yyyyMMdd') as int) time_key, "+
                            "cast(date_format(InvoiceDate,'yyyy') as int) year_id, "+
                            "cast(date_format(InvoiceDate,'yyyyMM') as int) month_id, "+
                            "SalesmanCode as transactional_salesman_code, "+
                            "InvoiceType as invoicetype, "+
                            "CustomerZRSSFlag as customer_ZRSS_flag, "+
                            "SalesReturnReferenceInvoiceNumber as sales_ret_ref_invoice_number, "+
                            "nvl((cast(GrossSalesValue as double) * 100),0) as gross_sales_val, "+
                            "nvl((cast(GrossSalesValue as double) * 100)/1000000,0) as gross_sales_val_mn, "+
                            "nvl(cast(NetInvoiceValue as double),0) as net_invoice_val, "+
                            "nvl(cast(GrossSalesWithReturnValue as double),0) as gross_sales_return_val, "+
                            "nvl(cast(SalesReturnValue as double),0) as sales_return_val, "+
                            "nvl(cast(OnInvoiceDiscountValue as double),0) as on_invoice_discount_val, "+
                            "nvl(cast(ValueAddedTax as double),0) value_added_tax, "+
                            "nvl(cast(OffInvoiceDiscountValue as double),0) off_inv_discount_val, "+
                            "nvl(cast(TurnoverValue as double),0) turn_over_val, "+
                            "nvl(cast(SalesWithReturnPCQuantity as int),0) sales_with_return_pc_qty, "+
                            "nvl(cast(SalesReturnPCQuantity as int),0) sales_return_pc_qty, "+
                            "nvl(cast(SalesPCQuantity as int),0) sales_pc_qty, "+
                            "nvl(cast(SalesCSVolume as double),0) sales_cs_vol, "+
                            "nvl(cast(SalesKGVolume as double),0) sales_kg_vol, "+
                            "nvl(cast(SalesLTVolume as double),0) sales_lt_vol, "+
                            "nvl(cast(FreePCQuantity as int),0) free_pc_qty, "+
                            "BDLLoadTimestamp, "+
                            "BDLUpdateTimestamp, "+
                            "YYYY, "+
                            "MM "+
                            "from online_invoice_sku_sales "+
                            "where CountryCode='VN'"
                          )
dataDSSPartitoned.createOrReplaceTempView("fact_dss")

# COMMAND ----------

display(dataDSSPartitoned)

# COMMAND ----------

#194870620
#195308694
dataDSSPartitoned.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC /* CHECKING IF DUPLICATE EXISTS */
# MAGIC select 
# MAGIC ArticleCode,
# MAGIC InvoiceNumber,
# MAGIC count(1) as tot_count
# MAGIC from online_invoice_sku_sales 
# MAGIC where YYYY ='2020' and MM='12' and DD='03'
# MAGIC and CountryCode="VN"
# MAGIC group by ArticleCode, InvoiceNumber
# MAGIC having count(1) > 1;

# COMMAND ----------

dataDSSPartitoned.filter(col("year_id")=="2018") \
                 .repartition(2) \
                 .write \
                 .partitionBy("year_id", "month_id", "time_key") \
                 .mode("append") \
                 .format("parquet") \
                 .option("compression","snappy") \
                 .save(dssProcPath)

# COMMAND ----------

dataDSSPartitoned.filter(col("year_id")=="2019") \
                 .repartition(2) \
                 .write \
                 .partitionBy("year_id", "month_id", "time_key") \
                 .mode("append") \
                 .format("parquet") \
                 .option("compression","snappy") \
                 .save(dssProcPath)

# COMMAND ----------

dataDSSPartitoned.filter(col("year_id")=="2020") \
                 .repartition(2) \
                 .write \
                 .partitionBy("year_id", "month_id", "time_key") \
                 .mode("append") \
                 .format("parquet") \
                 .option("compression","snappy") \
                 .save(dssProcPath)

# COMMAND ----------

dataDSSPartitoned.filter(col("year_id")=="2021") \
                 .repartition(2) \
                 .write \
                 .partitionBy("year_id", "month_id", "time_key") \
                 .mode("append") \
                 .format("parquet") \
                 .option("compression","snappy") \
                 .save(dssProcPath)
