# Databricks notebook source
#a fix needed to be done - need to read only the latest month's daily sales data and not the entire daily sales data and remove the where clause wile aggregating the data

import pandas as pd
from pyspark.sql.functions \
import col, date_format, unix_timestamp, from_unixtime, date_sub

# COMMAND ----------

def threeMonthID (yearID, monthID):
  yyyy= int(yearID)
  mm= int(monthID)
  if monthID == '01':
    yyyy= yyyy - 1
    mm= 11
    return str(yyyy)+str(mm)
  elif monthID == '02':
    yyyy= yyyy - 1
    mm= 12
    return str(yyyy)+str(mm)
  else:
    return str(yyyy)+ str((mm-3)+1).zfill(2)

yearID= int(dbutils.widgets.get("ProcessingYear"))
monthID= int(str(yearID) +dbutils.widgets.get("ProcessingMonth"))
monthPrevThree = threeMonthID (dbutils.widgets.get("ProcessingYear"), dbutils.widgets.get("ProcessingMonth"))

# COMMAND ----------

deltaMthlyAggPath   = "/mnt/adls/TTS/transformed/aggregates/fact_monthly_sales_agg"
deltaPath    = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Transactional/fact_daily_sales/"
pathDimChannel= "/mnt/adls/TTS/transformed/dimensions/dim_channel"
pathDimPromotion= "/mnt/adls/TTS/transformed/dimensions/dim_promotion"
pathDimCodedConsolidated= "/mnt/adls/TTS/transformed/dimensions/dim_coded_consolidated"
pathDimDistributor= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Distributor/"
pathDimOutlet= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Outlet/"
pathDimTime= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Time"
dssMonthlyTempPath='/mnt/adls/TTS/temp/temp_fact_monthly_sales_agg'

# COMMAND ----------

yearTD = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimDistributor)]))
monthTD= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimDistributor +"/" +str(yearTD))])).zfill(2)
dayTD  = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimDistributor +"/"+str(yearTD)+"/"+str(monthTD))])).zfill(2)

yearDT = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimTime)]))
monthDT= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimTime +"/" +str(yearDT))])).zfill(2)
dayDT  = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimTime +"/"+str(yearDT)+"/"+str(monthDT))])).zfill(2)


distributorDF= spark.read.option("header","true") \
                         .option("inferSchema","true") \
                         .csv(pathDimDistributor+yearTD+"/"+monthTD+"/"+dayTD)

outletDF= spark.read.option("header","true") \
                         .option("inferSchema","true") \
                         .csv(pathDimOutlet)

dimTimeDF= spark.read.option("header","true") \
                     .option("inferSchema","true") \
                     .csv(pathDimTime+"/"+yearDT+"/"+monthDT+"/"+dayDT)

dimCodedDF= spark.read.parquet(pathDimCodedConsolidated)

mappingChannelDF = spark.read.parquet(pathDimChannel)

outletDF.createOrReplaceTempView ("dim_outlet")
distributorDF.createOrReplaceTempView ("dim_distributor")
mappingChannelDF.createOrReplaceTempView("dim_channel")
dimCodedDF.createOrReplaceTempView("dim_coded_consolidated")
dimTimeDF.createOrReplaceTempView("dim_time")

# COMMAND ----------

# MAGIC %run /Shared/TTS/GET_SiteAndDistributorDT

# COMMAND ----------

# dssDF= spark.sql("""
#                             select *
#                             from fact_daily_sales
#                             where year_id= {} and month_id= {}
#                            """.format(yearID, monthID)
#                           )
# dssDF.createORreplaceTempView("fact_daily_sales")

data= spark.read.format('delta').load(deltaPath)
data.createOrReplaceTempView("fact_daily_sales")


dssRawDFDT = spark.sql("select dss.* "+
                       "from fact_daily_sales dss "+
                       "inner join list_site dt "+
                       "on dss.transactional_site_code = dt.site_code ")
dssRawDFDT.createOrReplaceTempView('temp_fact_daily_sales')

# COMMAND ----------

map_channel= {'PROFESSIONAL':'PROFESSIONAL',
              'MOM & POP':'MOM & POP',
              'F. GROCERY':'FAMILY GROCER',
              'WHOLESALER':'WHOLESALER',
              'DT H&B':'DT DRUG',
              'MARKET STALL':'MARKET STALLS'
}

mapChnPD = pd.DataFrame(list(map_channel.items()),columns = ['channel_org','channel'])
mapChnDF = spark.createDataFrame(mapChnPD)
mapChnDF.createOrReplaceTempView("ref_channel_map")

# COMMAND ----------

calculateAvgL3MGSV = spark.sql("""
                                select
                                outlet_code,
                                avg(gross_sales_val_mn) avg_gross_sales_mn
                                from
                                (select 
                                  outlet_code,
                                  month_id,
                                  sum(gross_sales_val_mn) gross_sales_val_mn
                                  from
                                  (select 
                                      transactional_outlet_code as outlet_code,
                                      month_id,
                                      gross_sales_val_mn
                                    from temp_fact_daily_sales dss
                                    where month_id between {} and {}
                                    union all
                                    select 
                                    outlet_code,
                                    year_month as month_id,
                                    0 as gross_sales_val_mn
                                    from
                                    (select distinct transactional_outlet_code as outlet_code
                                     from temp_fact_daily_sales where month_id between {} and {}
                                    ),
                                    (select distinct year_month
                                     from dim_time where year_month between {} and {}
                                    )
                                  )
                                  group by outlet_code, month_id
                                ) group by outlet_code
                               """.format(monthPrevThree, monthID, monthPrevThree, monthID, monthPrevThree, monthID)
                              )
calculateAvgL3MGSV.createOrReplaceTempView("ref_outlet_avgL3Mgsv")

# COMMAND ----------

# MAGIC %md ##### call standard customized product hierarchy

# COMMAND ----------

# MAGIC %run ./GET_StandardProductHierarchy

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists edge.fact_monthly_sales_agg;
# MAGIC CREATE TABLE edge.fact_monthly_sales_agg
# MAGIC (country_code string,
# MAGIC product_code string,
# MAGIC standard_product_group_code string,
# MAGIC region string,
# MAGIC invoice_number string,
# MAGIC site_code_year string,
# MAGIC transactional_distributor_code string,
# MAGIC transactional_site_code string,
# MAGIC transactional_outlet_code string,
# MAGIC channel_code int,
# MAGIC store_size_code int,
# MAGIC perfect_store_status_code int,
# MAGIC region_code int,
# MAGIC banded_code int,
# MAGIC invoice_date date,
# MAGIC transactional_salesman_code string,
# MAGIC invoicetype string,
# MAGIC customer_ZRSS_flag string,
# MAGIC --sales_ret_ref_invoice_number string,
# MAGIC gross_sales_val double,
# MAGIC gross_sales_val_mn double,
# MAGIC net_invoice_val double,
# MAGIC gross_sales_return_val double,
# MAGIC sales_return_val double,
# MAGIC on_invoice_discount_val double,
# MAGIC value_added_tax double,
# MAGIC off_inv_discount_val double,
# MAGIC turn_over_val double,
# MAGIC sales_with_return_pc_qty bigint,
# MAGIC sales_return_pc_qty bigint,
# MAGIC sales_pc_qty bigint,
# MAGIC sales_cs_vol double,
# MAGIC sales_kg_vol double,
# MAGIC sales_lt_vol double,
# MAGIC free_pc_qty bigint,
# MAGIC salesforce string,
# MAGIC brand string,
# MAGIC category_code string,
# MAGIC brand_cat_core string,
# MAGIC banded string,
# MAGIC year_id int,
# MAGIC month_id int,
# MAGIC time_key int
# MAGIC )
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (year_id, month_id)
# MAGIC LOCATION "/mnt/adls/TTS/transformed/aggregates/fact_monthly_sales_agg";
# MAGIC --"/mnt/adls/EDGE_Analytics/Datalake/Transformed/Transactional/fact_daily_sales";
# MAGIC --"/mnt/adls/TTS/transformed/facts/fact_daily_sales/";
# MAGIC 
# MAGIC ALTER TABLE edge.fact_monthly_sales_agg
# MAGIC SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);
# MAGIC 
# MAGIC -- CONVERT TO DELTA parquet.`/mnt/adls/EDGE_Analytics/Datalake/Transformed/Transactional/fact_daily_sales`
# MAGIC -- PARTITIONED BY (year_id int, month_id int, time_key int);

# COMMAND ----------

# transformedDF=spark.sql("select "+
#                         "txn.country_code, "+
#                         "txn.product_code, "+
#                         "cast(rsh.standard_product_group_code as string) standard_product_group_code, "+
#                         "dt.dt_region as region, "+
#                         "txn.invoice_number, "+
#                         "txn.site_code_year, "+
#                         "txn.transactional_distributor_code, "+
#                         "txn.transactional_site_code, "+
#                         "txn.transactional_outlet_code, "+
#                         "ol.channel, "+
#                         "0 region_code, "+
#                         "nvl(case when rsh.banded ='Banded' then 1 else 0 end,0) banded_code, "+
#                         "txn.invoice_date, "+
#                         "txn.transactional_salesman_code, "+
#                         "txn.invoicetype, "+
#                         "txn.customer_ZRSS_flag, "+
#                         "sum(txn.gross_sales_val) as gross_sales_val, "+
#                         "sum(txn.gross_sales_val_mn) as gross_sales_val_mn, "+
#                         "sum(txn.net_invoice_val) as net_invoice_val, "+
#                         "sum(txn.gross_sales_return_val) as gross_sales_return_val, "+
#                         "sum(txn.sales_return_val) as sales_return_val, "+
#                         "sum(txn.on_invoice_discount_val) as on_invoice_discount_val, "+
#                         "sum(txn.value_added_tax) as value_added_tax, "+
#                         "sum(txn.off_inv_discount_val) as off_inv_discount_val, "+
#                         "sum(txn.turn_over_val) as turn_over_val, "+
#                         "sum(txn.sales_with_return_pc_qty) as sales_with_return_pc_qty, "+
#                         "sum(txn.sales_return_pc_qty) as sales_return_pc_qty, "+
#                         "sum(txn.sales_pc_qty) as sales_pc_qty, "+
#                         "sum(txn.sales_cs_vol) as sales_cs_vol, "+
#                         "sum(txn.sales_kg_vol) as sales_kg_vol, "+
#                         "sum(txn.sales_lt_vol) as sales_lt_vol, "+
#                         "sum(txn.free_pc_qty) as free_pc_qty, "+
#                         "txn.salesforce, "+
#                         "txn.brand, "+
#                         "txn.category_code, "+
#                         "txn.brand_cat_core, "+
#                         "nvl(rsh.banded,'NA') as banded, "+
#                         "txn.year_id, "+
#                         "txn.month_id, "+
#                         "date_format(last_day(to_date (cast(txn.time_key as string),'yyyyMMdd')),'yyyyMMdd') time_key "+
#                         "from "+
#                         "(select * from fact_daily_sales where month_id=monthID) txn "+
#                         "left outer join "+
#                         "dim_distributor dt "+
#                         "on txn.transactional_distributor_code=dt.distributor_code "+
#                         "left outer join "+
#                         "(select distinct product_code,banded,standard_product_group_code from ref_standard_hierarchy) rsh "+
#                         "on txn.product_code= rsh.product_code "+
#                         "left outer join "+
#                         "(select "+
#                             "distinct "+
#                             "ol.outlet_code, "+
#                             "case when upper(chn.group_channel) ='PROFESSIONAL' then 'PROFESSIONAL' "+
#                                  "else chn.channel "+
#                                  "end channel "+
#                           "from dim_outlet ol "+
#                           "inner join dim_channel chn on chn.channel= ol.channel "+
#                         ") ol on txn.transactional_outlet_code = ol.outlet_code "+
#                         "group by "+
#                         "txn.product_code, "+
#                         "txn.country_code, "+
#                         "rsh.standard_product_group_code, "+
#                         "dt.dt_region as region, "+
#                         "txn.invoice_number, "+
#                         "txn.site_code_year, "+
#                         "txn.transactional_distributor_code, "+
#                         "txn.transactional_site_code, "+
#                         "txn.transactional_outlet_code, "+
#                         "ol.channel, "+
#                         "region_code, "+
#                         "nvl(case when rsh.banded ='Banded' then 1 else 0 end,0) banded_code, "+
#                         "txn.invoice_date, "+
#                         "txn.transactional_salesman_code, "+
#                         "txn.invoicetype, "+
#                         "txn.customer_ZRSS_flag, "+
#                         "txn.salesforce, "+
#                         "txn.brand, "+
#                         "txn.category_code, "+
#                         "txn.brand_cat_core, "+
#                         "banded, "+
#                         "txn.year_id, "+
#                         "txn.month_id, "+
#                         "date_format(last_day(to_date (cast(txn.time_key as string),'yyyyMMdd')),'yyyyMMdd') time_key")
# transformedDF.createOrReplaceTempView("mthly_sales")

# COMMAND ----------

transformedDF=spark.sql("""select 
txn.country_code,
txn.product_code,
cast(rsh.standard_product_group_code as string) standard_product_group_code,
dt.dt_region as region,
txn.invoice_number,
txn.site_code_year,
txn.transactional_distributor_code,
txn.transactional_site_code,
txn.transactional_outlet_code,
ol.channel,
0 region_code,
nvl(case when rsh.banded ='Banded' then 1 else 0 end,0) banded_code,
txn.invoice_date,
txn.transactional_salesman_code,
txn.invoicetype,
txn.customer_ZRSS_flag,
sum(txn.gross_sales_val) as gross_sales_val,
sum(txn.gross_sales_val_mn) as gross_sales_val_mn,
sum(txn.net_invoice_val) as net_invoice_val,
sum(txn.gross_sales_return_val) as gross_sales_return_val,
sum(txn.sales_return_val) as sales_return_val,
sum(txn.on_invoice_discount_val) as on_invoice_discount_val,
sum(txn.value_added_tax) as value_added_tax,
sum(txn.off_inv_discount_val) as off_inv_discount_val,
sum(txn.turn_over_val) as turn_over_val,
sum(txn.sales_with_return_pc_qty) as sales_with_return_pc_qty,
sum(txn.sales_return_pc_qty) as sales_return_pc_qty,
sum(txn.sales_pc_qty) as sales_pc_qty,
sum(txn.sales_cs_vol) as sales_cs_vol,
sum(txn.sales_kg_vol) as sales_kg_vol,
sum(txn.sales_lt_vol) as sales_lt_vol,
sum(txn.free_pc_qty) as free_pc_qty,
txn.salesforce,
txn.brand,
txn.category_code,
txn.brand_cat_core,
nvl(rsh.banded,'NA') as banded,
txn.year_id,
txn.month_id,
date_format(last_day(to_date (cast(txn.time_key as string),'yyyyMMdd')),'yyyyMMdd') time_key
from
(select * from temp_fact_daily_sales where month_id={}) txn
left outer join
dim_distributor dt
on txn.transactional_distributor_code=dt.distributor_code
left outer join
(select distinct product_code,banded,standard_product_group_code from ref_standard_hierarchy) rsh
on txn.product_code= rsh.product_code
left outer join
(select
    distinct
    ol.outlet_code,
    case when upper(chn.group_channel) ='PROFESSIONAL' then 'PROFESSIONAL'
         else chn.channel
         end channel
  from dim_outlet ol
  inner join dim_channel chn on chn.channel= ol.channel
  ) ol on txn.transactional_outlet_code = ol.outlet_code
group by
txn.product_code,
txn.country_code,
rsh.standard_product_group_code,
region,
txn.invoice_number,
txn.site_code_year,
txn.transactional_distributor_code,
txn.transactional_site_code,
txn.transactional_outlet_code,
ol.channel,
region_code,
banded_code,
txn.invoice_date,
txn.transactional_salesman_code,
txn.invoicetype,
txn.customer_ZRSS_flag,
txn.salesforce,
txn.brand,
txn.category_code,
txn.brand_cat_core,
banded,
txn.year_id,
txn.month_id,
time_key""".format(monthID))
transformedDF.createOrReplaceTempView('mthly_sales')

# COMMAND ----------

# %sql
# select distinct product_code,  standard_product_group_code
# from mthly_sales where month_id=202001
# and (product_code is not null and standard_product_group_code is null) and product_code not like '75%';

# COMMAND ----------

# %sql
# select * from dim_local_product where product_code  in (
#   select distinct product_code
#   from mthly_sales where month_id=202001
#   and (product_code is not null and standard_product_group_code is null)
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists coded_transformedAgg;
# MAGIC create temporary view coded_transformedAgg as
# MAGIC with get_channel_code (
# MAGIC  select 
# MAGIC     chmap.channel_org as channel,
# MAGIC     chn.ref_code
# MAGIC   from ref_channel_map as chmap
# MAGIC   left join (select * from dim_coded_consolidated where ref_type='CHANNEL') chn
# MAGIC   on chmap.channel_org = chn.ref_desc
# MAGIC ),
# MAGIC get_store_size_code (
# MAGIC   select 
# MAGIC     lkp.outlet_code,
# MAGIC     lkp.avg_gross_sales_mn,
# MAGIC     ref_desc,
# MAGIC     ref_code as store_size_code
# MAGIC   from ref_outlet_avgL3Mgsv lkp
# MAGIC   left outer join (select * from dim_coded_consolidated where ref_type='STORE_SIZE_CLASS') store
# MAGIC   on lkp.avg_gross_sales_mn between store.param1 and store.param2
# MAGIC ),
# MAGIC perfect_store_status(
# MAGIC   select 
# MAGIC     ol.outlet_code,
# MAGIC     ol.perfect_store,
# MAGIC     pss.ref_code
# MAGIC   from dim_outlet as ol
# MAGIC   left outer join 
# MAGIC   (select 
# MAGIC     (case when dcc.ref_desc='Yes' then 'Y' else 'N' end) as ref_desc,
# MAGIC      ref_code 
# MAGIC      from dim_coded_consolidated dcc where ref_type='PERFECT_STORE') as pss
# MAGIC   on ol.perfect_store=pss.ref_desc
# MAGIC )
# MAGIC select
# MAGIC   txn.country_code,
# MAGIC   nvl(txn.product_code,'NA') as product_code,
# MAGIC   txn.standard_product_group_code,
# MAGIC   txn.region,
# MAGIC   txn.invoice_number,
# MAGIC   txn.site_code_year,
# MAGIC   txn.transactional_distributor_code,
# MAGIC   txn.transactional_site_code,
# MAGIC   txn.transactional_outlet_code,
# MAGIC   nvl(cast(chn.ref_code as int),0) as channel_code,
# MAGIC   cast(ssc.store_size_code as int) as store_size_code,
# MAGIC   cast(pss.ref_code as int) as perfect_store_status_code,
# MAGIC   cast(txn.region_code as int) as region_code,
# MAGIC   cast(txn.banded_code as int) as banded_code,
# MAGIC   txn.invoice_date,
# MAGIC   txn.transactional_salesman_code,
# MAGIC   txn.invoicetype,
# MAGIC   txn.customer_ZRSS_flag,
# MAGIC   txn.gross_sales_val,
# MAGIC   txn.gross_sales_val_mn,
# MAGIC   txn.net_invoice_val,
# MAGIC   txn.gross_sales_return_val,
# MAGIC   txn.sales_return_val,
# MAGIC   txn.on_invoice_discount_val,
# MAGIC   txn.value_added_tax,
# MAGIC   txn.off_inv_discount_val,
# MAGIC   txn.turn_over_val,
# MAGIC   txn.sales_with_return_pc_qty,
# MAGIC   txn.sales_return_pc_qty,
# MAGIC   txn.sales_pc_qty,
# MAGIC   txn.sales_cs_vol,
# MAGIC   txn.sales_kg_vol,
# MAGIC   txn.sales_lt_vol,
# MAGIC   txn.free_pc_qty,
# MAGIC   txn.salesforce,
# MAGIC   txn.brand,
# MAGIC   txn.category_code,
# MAGIC   txn.brand_cat_core,
# MAGIC   txn.banded,
# MAGIC   txn.year_id,
# MAGIC   txn.month_id,
# MAGIC   cast(txn.time_key as int) time_key
# MAGIC from
# MAGIC (select * from mthly_sales) as txn
# MAGIC left outer join
# MAGIC get_channel_code as chn
# MAGIC on txn.channel= chn.channel
# MAGIC left outer join
# MAGIC get_store_size_code as ssc
# MAGIC on txn.transactional_outlet_code = ssc.outlet_code
# MAGIC left outer join
# MAGIC perfect_store_status as pss
# MAGIC on txn.transactional_outlet_code=pss.outlet_code

# COMMAND ----------

# coded_tranformedDF=spark.sql("select "+
#                         "txn.country_code, "+
#                         "txn.product_code, "+
#                         "txn.invoice_number, "+
#                         "txn.site_code_year, "+
#                         "txn.transactional_distributor_code, "+
#                         "txn.transactional_site_code, "+
#                         "txn.transactional_outlet_code, "+
#                         "chn.ref_code as channel_code, "+ 
#                         "ssc.store_size_code, "     
#                         "txn.invoice_date, "+
#                         "txn.transactional_salesman_code, "+
#                         "txn.invoicetype, "+
#                         "txn.customer_ZRSS_flag, "+
#                         "txn.gross_sales_val, "+
#                         "txn.gross_sales_val_mn, "+
#                         "txn.net_invoice_val, "+
#                         "txn.gross_sales_return_val, "+
#                         "txn.sales_return_val, "+
#                         "txn.on_invoice_discount_val, "+
#                         "txn.value_added_tax, "+
#                         "txn.off_inv_discount_val, "+
#                         "txn.turn_over_val, "+
#                         "txn.sales_with_return_pc_qty, "+
#                         "txn.sales_return_pc_qty, "+
#                         "txn.sales_pc_qty, "+
#                         "txn.sales_cs_vol, "+
#                         "txn.sales_kg_vol, "+
#                         "txn.sales_lt_vol, "+
#                         "txn.free_pc_qty, "+
#                         "sm.salesforce, "+
#                         "txn.brand, "+
#                         "txn.category_code, "+
#                         "txn.brand_cat_core, "+
#                         "txn.year_id, "+
#                         "txn.month_id "+
#                         "from "+
#                         "(select * from daily_sales_agg) txn "+
#                         "left outer join "+
#                         "get_channel_code chn "+
#                         "on txn.channel= chn.ref_code "+
#                         "left outer join "+
#                         "get_store_size_code ssc "+
#                         "on txn.transactional_outlet_code = ssc.outlet_code "+
#                         "left outer join "+ 
#                         "(select * from dim_coded_consolidated where ref_type='PERFECT_STORE') "+
#                         "on txn"     

# COMMAND ----------

coded_transformedDF=spark.sql("""select * from coded_transformedAgg where product_code not like '75%'""")

# COMMAND ----------

coded_transformedDF.repartition(10) \
             .write \
             .format("delta") \
             .mode("overwrite") \
             .partitionBy("year_id", "month_id") \
             .save(deltaMthlyAggPath)

# COMMAND ----------

(spark.sql("select * from edge.fact_monthly_sales_agg where month_id={}".format(monthID))
      .repartition(30)
      .write
      .mode("overwrite")
      .parquet (dssMonthlyTempPath)
)

# COMMAND ----------

# REMOVING NON PART FILES BEFORE INITIATING COPY ACTIVITY USING POLYBASE
fileInfo= dbutils.fs.ls(dssMonthlyTempPath)
fileList= [str(i.name) for i in fileInfo]

def filterNonPartFiles (fileName):
  if 'part' in fileName:
    return False
  else:
    return True

nonPartFiles = list(filter(filterNonPartFiles, fileList))

for file in nonPartFiles:
  dbutils.fs.rm(dssMonthlyTempPath+"/{}".format(file))

# COMMAND ----------


