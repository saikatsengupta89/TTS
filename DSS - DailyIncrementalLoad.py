# Databricks notebook source
# MAGIC %md ##### necessary imports and variable substitution

# COMMAND ----------

from pyspark.sql.functions \
import col, date_format, unix_timestamp, from_unixtime, date_sub
import datetime as dt

spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

timeKey= str(dbutils.widgets.get("ProcessingTimeKey"))
timeKeyPrevDay= (dt.datetime.strptime(timeKey,'%Y%m%d') - dt.timedelta(1)).strftime('%Y%m%d')
timeKeyST = (dt.datetime.strptime(timeKey,'%Y%m%d') - dt.timedelta(6)).strftime('%Y%m%d')
timeKeyED = timeKey
yyyy= timeKey[:4]
mm= timeKey[4:6]
dd= timeKey[6:]
# print(yyyy, mm, dd)
# print(timeKeyPrevDay, timeKeyST, timeKeyED)

# COMMAND ----------

# MAGIC %md ##### define usable paths and create dimension dataframes

# COMMAND ----------

bdlDeltaPath="/mnt/adls/BDL_Gen2/BusinessDataLake/CD/SecSales/Global/Online_Countries/Transactional/OnlineInvoiceSKUSales/OnlineInvoiceSKUSales_hist_parquet/"
bdlDeltaPathIncr="/mnt/adls/BDL_Gen2/BusinessDataLake/CD/SecSales/Global/Online_Countries/Transactional/OnlineInvoiceSKUSales/OnlineInvoiceSKUSales_parquet/"
salesmanPath= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Salesman/"
prodPath    = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Product/"
dssPrcPath = "/mnt/adls/EDGE_Analytics/Datalake/Processed/transactions/daily/txn_daily_sales"
dssTrnPath = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Transactional/fact_daily_sales"
dssTmpPath = "/mnt/adls/EDGE_Analytics/Datalake/temp/temp_fact_daily_sales"

# COMMAND ----------

yearSM = str(max([i.name.replace('/','') for i in dbutils.fs.ls (salesmanPath)]))
monthSM= str(max([i.name.replace('/','') for i in dbutils.fs.ls (salesmanPath +"/" +str(yearSM))]))
daySM  = str(max([i.name.replace('/','') for i in dbutils.fs.ls (salesmanPath +"/"+str(yearSM)+"/"+str(monthSM))]))

yearPD = str(max([i.name.replace('/','') for i in dbutils.fs.ls (prodPath)]))
monthPD= str(max([i.name.replace('/','') for i in dbutils.fs.ls (prodPath +"/" +str(yearPD))]))
dayPD  = str(max([i.name.replace('/','') for i in dbutils.fs.ls (prodPath +"/"+str(yearPD)+"/"+str(monthPD))]))

dataSM= spark.read.option("header","true").csv(salesmanPath+yearSM+"/"+monthSM+"/"+daySM)
dataSM.createOrReplaceTempView ("dim_salesman")

dataPD= spark.read.option("header","true").csv(prodPath+yearPD+"/"+monthPD+"/"+dayPD)
dataPD.createOrReplaceTempView ("dim_product")

# COMMAND ----------

# MAGIC %md ##### PDS processed layer - read and process daily sales incremental data from BDL delta lake

# COMMAND ----------

# READ AND PROCESS DAILY SALES INCREMENTAL DATA FROM BDL DELTA TO PDS PROCESSED LAYER
dssBdlDFHist = spark.read \
                    .format("delta") \
                    .load(bdlDeltaPath)
dssBdlDFHist.createOrReplaceTempView("online_invoice_sku_sales")

dataDSSIncremental=spark.sql("select " +
                            "CountryCode as country_code, "+
                            "substring(ArticleCode, -8, 8) as product_code, "+
                            "InvoiceNumber as invoice_number, "+
                            "concat(SiteCode,'_',date_format(InvoiceDate, 'yyyy')) site_code_year, "+
                            "DistributorCode as transactional_distributor_code, "+
                            "SiteCode as transactional_site_code, "+
                            "OutletCode as transactional_outlet_code, "+
                            "InvoiceDate as invoice_date, "+
#                           below three custom fields are created for partitioning in the data lake
                            "cast(concat(YYYY,MM,DD) as int) time_key, "+
                            "cast(YYYY as int) year_id, "+
                            "cast(concat(YYYY,MM) as int) month_id, "+
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
                            "BDLUpdateTimestamp "+
                            "from online_invoice_sku_sales "+
                            "where CountryCode='VN' "+
                            "and YYYY={} and MM={} and DD={}".format(yyyy, mm, dd)
                          )
dataDSSIncremental.createOrReplaceTempView("processed_daily_sales")
(
dataDSSIncremental#.repartition(2)
                  .write
                  .partitionBy("year_id", "month_id", "time_key")
                  .mode("overwrite")
                  .format("parquet")
                  .option("compression","snappy")
                  .save(dssPrcPath)
)

# COMMAND ----------

# MAGIC %md ##### read daily sales incremental data from PDS processed layer and enrich it with required dimensional attributes

# COMMAND ----------

# READ AND TRANSFORM DAILY SALES INCREMENTAL DATA FROM PDS PROCESSED TO TRANSFORMED LAYER

# READ DAILY SALES INCREMENTAL DATA FROM PROCESSED LAYER
dssProcessed = spark.read\
                    .option("basePath",dssPrcPath) \
                    .parquet(dssPrcPath+"/year_id="+yyyy+"/month_id="+str(yyyy+mm)+"/time_key="+str(yyyy+mm+dd))
dssProcessed.createOrReplaceTempView ("fact_daily_sales_processed")

transformedDF=spark.sql("select "+
                        "txn.country_code, "+
                        "txn.product_code, "+
                        "txn.invoice_number, "+
                        "txn.site_code_year, "+
                        "txn.transactional_distributor_code, "+
                        "txn.transactional_site_code, "+
                        "txn.transactional_outlet_code, "+
                        "txn.invoice_date, "+
                        "txn.transactional_salesman_code, "+
                        "txn.invoicetype, "+
                        "txn.customer_ZRSS_flag, "+
                        "txn.sales_ret_ref_invoice_number, "+
                        "txn.gross_sales_val, "+
                        "txn.gross_sales_val_mn, "+
                        "txn.net_invoice_val, "+
                        "txn.gross_sales_return_val, "+
                        "txn.sales_return_val, "+
                        "txn.on_invoice_discount_val, "+
                        "txn.value_added_tax, "+
                        "txn.off_inv_discount_val, "+
                        "txn.turn_over_val, "+
                        "txn.sales_with_return_pc_qty, "+
                        "txn.sales_return_pc_qty, "+
                        "txn.sales_pc_qty, "+
                        "txn.sales_cs_vol, "+
                        "txn.sales_kg_vol, "+
                        "txn.sales_lt_vol, "+
                        "txn.free_pc_qty, "+
                        "sm.salesforce, "+
                        "pd.brand, "+
                        "pd.category_code, "+
                        "pd.brand_cat_core, "+
                        "date_format(txn.invoice_date,'yyyy') year_id, "+
                        "date_format(txn.invoice_date,'yyyyMM') month_id, "+
                        "date_format(txn.invoice_date,'yyyyMMdd') time_key, "+
                        "current_timestamp() as insert_ts, "+
                        "current_timestamp() as update_ts "+
                        "from "+
                        "fact_daily_sales_processed txn "+
                        "left outer join "+
                        "(select distinct salesman_code, salesforce from dim_salesman) sm "+
                        "on txn.transactional_salesman_code= sm.salesman_code "+
                        "left outer join "+
                        "(select "+
                        "   distinct product_code, brand, category_code, cotc, concat(brand,'_',category_code,'_',cotc) brand_cat_core "+
                        "   from dim_product "+
                        ") pd on txn.product_code= pd.product_code")
transformedDF.createOrReplaceTempView("fact_daily_sales_transformed")

# COMMAND ----------

# MAGIC %md ##### PDS transformed layer - upsert transformed data into daily sales delta lake 

# COMMAND ----------

# READ TRANSFORMED DATA AND UPSERT INTO DAILY SALES DELTA TABLE IN PDS

# CHECK IF DATA RELOAD HAPPENING OR NOT
reload_df = spark.sql("select * from fact_daily_sales where time_key={}".format(timeKey))

# DELETE THE LATEST INSERTED RECORDS BEFORE RELOADING THE DATA FOR THE PROCESSED DAY
if (reload_df.count() > 0):
  print("Reload. Deleting data for existing load")
  spark.sql ("""
             delete
              from fact_daily_sales
              where time_key between {} and {}
              and insert_ts in (select max(insert_ts) from fact_daily_sales where time_key between {} and {})
              """.format(timeKeyST, timeKeyED, timeKeyST, timeKeyED)
            )
else:
  print("Fresh Load")

# UPSERT DATA FOR PROCESSED DAY
spark.sql("""
merge into fact_daily_sales tgt
using fact_daily_sales_transformed src
on (
tgt.time_key in ({}, {})
and tgt.product_code = src.product_code
and tgt.invoice_number = src.invoice_number
)
when matched then update set  tgt.country_code= src.country_code,
                              tgt.product_code= src.product_code,
                              tgt.invoice_number= src.invoice_number,
                              tgt.site_code_year= src.site_code_year,
                              tgt.transactional_distributor_code= src.transactional_distributor_code,
                              tgt.transactional_site_code= src.transactional_site_code,
                              tgt.transactional_outlet_code= src.transactional_outlet_code,
                              tgt.transactional_salesman_code= src.transactional_salesman_code,
                              tgt.invoicetype= src.invoicetype,
                              tgt.customer_ZRSS_flag= src.customer_ZRSS_flag,
                              tgt.sales_ret_ref_invoice_number= src.sales_ret_ref_invoice_number,
                              tgt.gross_sales_val= src.gross_sales_val,
                              tgt.gross_sales_val_mn= src.gross_sales_val_mn,
                              tgt.net_invoice_val= src.net_invoice_val,
                              tgt.gross_sales_return_val= src.gross_sales_return_val,
                              tgt.sales_return_val= src.sales_return_val,
                              tgt.on_invoice_discount_val= src.on_invoice_discount_val,
                              tgt.value_added_tax= src.value_added_tax,
                              tgt.off_inv_discount_val= src.off_inv_discount_val,
                              tgt.turn_over_val= src.turn_over_val,
                              tgt.sales_with_return_pc_qty= src.sales_with_return_pc_qty,
                              tgt.sales_return_pc_qty= src.sales_return_pc_qty,
                              tgt.sales_pc_qty= src.sales_pc_qty,
                              tgt.sales_cs_vol= src.sales_cs_vol,
                              tgt.sales_kg_vol= src.sales_kg_vol,
                              tgt.sales_lt_vol= src.sales_lt_vol,
                              tgt.free_pc_qty= src.free_pc_qty,
                              tgt.salesforce= src.salesforce,
                              tgt.brand= src.brand,
                              tgt.category_code= src.category_code,
                              tgt.brand_cat_core= src.brand_cat_core,
                              tgt.update_ts= src.update_ts
when not matched then insert (country_code,
                              product_code,
                              invoice_number,
                              site_code_year,
                              transactional_distributor_code,
                              transactional_site_code,
                              transactional_outlet_code,
                              invoice_date,
                              transactional_salesman_code,
                              invoicetype,
                              customer_ZRSS_flag,
                              sales_ret_ref_invoice_number,
                              gross_sales_val,
                              gross_sales_val_mn,
                              net_invoice_val,
                              gross_sales_return_val,
                              sales_return_val,
                              on_invoice_discount_val,
                              value_added_tax,
                              off_inv_discount_val,
                              turn_over_val,
                              sales_with_return_pc_qty,
                              sales_return_pc_qty,
                              sales_pc_qty,
                              sales_cs_vol,
                              sales_kg_vol,
                              sales_lt_vol,
                              free_pc_qty,
                              salesforce,
                              brand,
                              category_code,
                              brand_cat_core,
                              year_id,
                              month_id,
                              time_key,
                              insert_ts,
                              update_ts) 
                             values
                             (src.country_code,
                              src.product_code,
                              src.invoice_number,
                              src.site_code_year,
                              src.transactional_distributor_code,
                              src.transactional_site_code,
                              src.transactional_outlet_code,
                              src.invoice_date,
                              src.transactional_salesman_code,
                              src.invoicetype,
                              src.customer_ZRSS_flag,
                              src.sales_ret_ref_invoice_number,
                              src.gross_sales_val,
                              src.gross_sales_val_mn,
                              src.net_invoice_val,
                              src.gross_sales_return_val,
                              src.sales_return_val,
                              src.on_invoice_discount_val,
                              src.value_added_tax,
                              src.off_inv_discount_val,
                              src.turn_over_val,
                              src.sales_with_return_pc_qty,
                              src.sales_return_pc_qty,
                              src.sales_pc_qty,
                              src.sales_cs_vol,
                              src.sales_kg_vol,
                              src.sales_lt_vol,
                              src.free_pc_qty,
                              src.salesforce,
                              src.brand,
                              src.category_code,
                              src.brand_cat_core,
                              src.year_id,
                              src.month_id,
                              src.time_key,
                              src.insert_ts,
                              src.update_ts)
""".format(timeKey, timeKeyPrevDay))

# COMMAND ----------

# MAGIC %md ##### PDS transformed layer - optimize daily sales delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize fact_daily_sales zorder by (transactional_outlet_code, transactional_distributor_code);
# MAGIC analyze table fact_daily_sales compute statistics;

# COMMAND ----------

# MAGIC %md ##### run return invoice merge on daily sales delta

# COMMAND ----------

# MAGIC %run "/Shared/EDGE_Analytics/DSS - MergeReturnInvoice"

# COMMAND ----------

# MAGIC %md ##### write incremental last 7 days data to temp location

# COMMAND ----------

(spark.sql("select * from fact_daily_sales where time_key between {} and {}".format(timeKeyST, timeKeyED))
      .repartition(30)
      .write
      .mode("overwrite")
      .parquet (dssTmpPath)
)

# REMOVING NON PART FILES BEFORE INITIATING COPY ACTIVITY USING POLYBASE
fileInfo= dbutils.fs.ls(dssTmpPath)
fileList= [str(i.name) for i in fileInfo]

def filterNonPartFiles (fileName):
  if 'part' in fileName:
    return False
  else:
    return True

nonPartFiles = list(filter(filterNonPartFiles, fileList))

for file in nonPartFiles:
  dbutils.fs.rm(dssTmpPath+"/{}".format(file))

# COMMAND ----------

# %sql
# VACUUM fact_daily_sales RETAIN 168 HOURS;

# COMMAND ----------

# BACKUP QUERIES
# %sql
# select time_key, count(1)
# from fact_daily_sales 
# where month_id=202012
# and (invoice_number, product_code) in (
# select sales_ret_ref_invoice_number, product_code
# from fact_daily_sales_transformed where sales_ret_ref_invoice_number !='0'
# )
# group by time_key;

#delete from fact_daily_sales where time_key=20201205 and update_ts =(select max(update_ts) from fact_daily_sales where time_key=20201205);
#delete from fact_daily_sales where time_key in (20201206, 20201207);

# select time_key, count(1)
# from fact_daily_sales
# where month_id=202012
# group by time_key

# select time_key, count(1)
# from processed_daily_sales
# where month_id=202012
# group by time_key;

# select time_key, count(1)
# from fact_daily_sales_transformed
# where month_id=202012
# group by time_key;

# %sql
# select update_ts, time_key, count(1)
# from fact_daily_sales
# where time_key between 20201202 and 20201208
# and insert_ts in (select max(insert_ts) from fact_daily_sales where time_key between 20201202 and 20201208)
# group by update_ts, time_key;
