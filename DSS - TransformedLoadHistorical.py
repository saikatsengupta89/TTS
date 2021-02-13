# Databricks notebook source
from pyspark.sql.functions \
import col, date_format, unix_timestamp, from_unixtime, date_sub

# COMMAND ----------

deltaPath   = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Transactional/fact_daily_sales"
procPath    = "/mnt/adls/EDGE_Analytics/Datalake/Processed/transactions/daily/txn_daily_sales"
salesmanPath= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Salesman/"
prodPath    = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Product/"

# COMMAND ----------

yearSM = str(max([i.name.replace('/','') for i in dbutils.fs.ls (salesmanPath)]))
monthSM= str(max([i.name.replace('/','') for i in dbutils.fs.ls (salesmanPath +"/" +str(yearSM))])).zfill(0)
daySM  = str(max([i.name.replace('/','') for i in dbutils.fs.ls (salesmanPath +"/"+str(yearSM)+"/"+str(monthSM))])).zfill(0)

yearPD = str(max([i.name.replace('/','') for i in dbutils.fs.ls (prodPath)]))
monthPD= str(max([i.name.replace('/','') for i in dbutils.fs.ls (prodPath +"/" +str(yearPD))])).zfill(0)
dayPD  = str(max([i.name.replace('/','') for i in dbutils.fs.ls (prodPath +"/"+str(yearPD)+"/"+str(monthPD))])).zfill(0)

# COMMAND ----------

dataSM= spark.read.option("header","true").csv(salesmanPath+yearSM+"/"+monthSM+"/"+daySM)
dataSM.createOrReplaceTempView ("dim_salesman")

dataPD= spark.read.option("header","true").csv(prodPath+yearPD+"/"+monthPD+"/"+dayPD)
dataPD.createOrReplaceTempView ("dim_product")

data= spark.read.parquet(procPath)
data.createOrReplaceTempView("txn_daily_sales")

# COMMAND ----------

dbutils.fs.rm("/mnt/adls/EDGE_Analytics/Datalake/Transformed/Transactional/fact_daily_sales",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists fact_daily_sales;
# MAGIC CREATE TABLE IF NOT EXISTS fact_daily_sales
# MAGIC (country_code string,
# MAGIC product_code string,
# MAGIC invoice_number string,
# MAGIC site_code_year string,
# MAGIC transactional_distributor_code string,
# MAGIC transactional_site_code string,
# MAGIC transactional_outlet_code string,
# MAGIC invoice_date date,
# MAGIC transactional_salesman_code string,
# MAGIC invoicetype string,
# MAGIC customer_ZRSS_flag string,
# MAGIC sales_ret_ref_invoice_number string,
# MAGIC gross_sales_val double,
# MAGIC gross_sales_val_mn double,
# MAGIC net_invoice_val double,
# MAGIC gross_sales_return_val double,
# MAGIC sales_return_val double,
# MAGIC on_invoice_discount_val double,
# MAGIC value_added_tax double,
# MAGIC off_inv_discount_val double,
# MAGIC turn_over_val double,
# MAGIC sales_with_return_pc_qty int,
# MAGIC sales_return_pc_qty int,
# MAGIC sales_pc_qty int,
# MAGIC sales_cs_vol double,
# MAGIC sales_kg_vol double,
# MAGIC sales_lt_vol double,
# MAGIC free_pc_qty int,
# MAGIC salesforce string,
# MAGIC brand string,
# MAGIC category_code string,
# MAGIC brand_cat_core string,
# MAGIC year_id int,
# MAGIC month_id int,
# MAGIC time_key int,
# MAGIC insert_ts timestamp,
# MAGIC update_ts timestamp
# MAGIC )
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (year_id, month_id, time_key)
# MAGIC LOCATION "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Transactional/fact_daily_sales";
# MAGIC --"/mnt/adls/EDGE_Analytics/Datalake/Transformed/Transactional/fact_daily_sales";
# MAGIC --"/mnt/adls/TTS/transformed/facts/fact_daily_sales/";
# MAGIC 
# MAGIC ALTER TABLE fact_daily_sales
# MAGIC SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
# MAGIC 
# MAGIC -- CONVERT TO DELTA parquet.`/mnt/adls/EDGE_Analytics/Datalake/Transformed/Transactional/fact_daily_sales`
# MAGIC -- PARTITIONED BY (year_id int, month_id int, time_key int);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted fact_daily_sales;

# COMMAND ----------

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
                        "txn.year_id, "+
                        "txn.month_id, "+
                        "txn.time_key, "+
                        "current_timestamp() as insert_ts, "+
                        "current_timestamp() as update_ts "+
                        "from "+
                        "txn_daily_sales txn "+
                        "left outer join "+
                        "(select distinct salesman_code, salesforce from dim_salesman) sm "+
                        "on txn.transactional_salesman_code= sm.salesman_code "+
                        "left outer join "+
                        "(select "+
                        "   distinct product_code, brand, category_code, cotc, concat(brand,'_',category_code,'_',cotc) brand_cat_core "+
                        "   from dim_product "+
                        ") pd on txn.product_code= pd.product_code")

# COMMAND ----------

transformedDF.createOrReplaceTempView("daily_sales")
display(spark.sql("select count(1) from daily_sales where nvl(transactional_salesman_code,'NA')='NA'"))

# COMMAND ----------

transformedDF.filter(col("year_id")=="2018") \
             .write \
             .format("delta") \
             .mode("append") \
             .partitionBy("year_id", "month_id", "time_key") \
             .save(deltaPath)

# COMMAND ----------

transformedDF.filter(col("year_id")=="2019") \
             .write \
             .format("delta") \
             .mode("append") \
             .partitionBy("year_id", "month_id", "time_key") \
             .save(deltaPath)

# COMMAND ----------

transformedDF.filter(col("year_id")=="2020") \
             .write \
             .format("delta") \
             .mode("append") \
             .partitionBy("year_id", "month_id", "time_key") \
             .save(deltaPath)

# COMMAND ----------

transformedDF.filter(col("year_id")=="2021") \
             .write \
             .format("delta") \
             .mode("append") \
             .partitionBy("year_id", "month_id", "time_key") \
             .save(deltaPath)

# COMMAND ----------

# MAGIC %sql
# MAGIC analyze table fact_daily_sales compute statistics;
# MAGIC optimize fact_daily_sales zorder by (transactional_outlet_code, transactional_distributor_code);

# COMMAND ----------


