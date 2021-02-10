# Databricks notebook source
import pandas as pd

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

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

pathDimChannel= "/mnt/adls/TTS/transformed/dimensions/dim_channel"
pathDimPromotion= "/mnt/adls/TTS/transformed/dimensions/dim_promotion"
pathDimCodedConsolidated= "/mnt/adls/TTS/transformed/dimensions/dim_coded_consolidated"
pathDimDistributor= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Distributor"
pathDimOutlet= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Outlet"
pathDimTime= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Time"
deltaPathMonthlyPromo="/mnt/adls/TTS/transformed/aggregates/fact_monthly_promo_agg"
nrmMonthlyTempPath="/mnt/adls/TTS/temp/temp_fact_monthly_promo_agg"

# COMMAND ----------

yearTD = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimDistributor)]))
monthTD= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimDistributor +"/" +str(yearTD))])).zfill(2)
dayTD  = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimDistributor +"/"+str(yearTD)+"/"+str(monthTD))])).zfill(2)

yearDT = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimTime)]))
monthDT= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimTime +"/" +str(yearDT))])).zfill(2)
dayDT  = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimTime +"/"+str(yearDT)+"/"+str(monthDT))])).zfill(2)

distributorDF= spark.read.option("header","true") \
                         .option("inferSchema","true") \
                         .csv(pathDimDistributor+"/"+yearTD+"/"+monthTD+"/"+dayTD)

outletDF= spark.read.option("header","true") \
                         .option("inferSchema","true") \
                         .csv(pathDimOutlet)

dimTimeDF= spark.read.option("header","true") \
                     .option("inferSchema","true") \
                     .csv(pathDimTime+"/"+yearDT+"/"+monthDT+"/"+dayDT)

dimPromotionDF= spark.read.parquet(pathDimPromotion)

dimCodedDF= spark.read.parquet(pathDimCodedConsolidated)

mappingChannelDF = spark.read.parquet(pathDimChannel)

nrmPromotionsDF= spark.sql("""
                            select *
                            from edge.fact_daily_promotions
                            where year_id= {} and month_id= {}
                           """.format(yearID, monthID)
                          )
outletDF.createOrReplaceTempView ("dim_outlet")
distributorDF.createOrReplaceTempView ("dim_distributor")
dimPromotionDF.createOrReplaceTempView("dim_promotion")
mappingChannelDF.createOrReplaceTempView("dim_channel")
dimCodedDF.createOrReplaceTempView("dim_coded_consolidated")
nrmPromotionsDF.createOrReplaceTempView("fct_daily_promotions")
dimTimeDF.createOrReplaceTempView("dim_time")

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
                                    from fact_daily_sales dss
                                    where month_id between {} and {}
                                    union all
                                    select 
                                    outlet_code,
                                    year_month as month_id,
                                    0 as gross_sales_val_mn
                                    from
                                    (select distinct transactional_outlet_code as outlet_code
                                     from fact_daily_sales where month_id between {} and {}
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

# MAGIC %sql
# MAGIC drop view if exists fact_monthly_promotions_on;
# MAGIC create temporary view fact_monthly_promotions_on as
# MAGIC with get_channel_code (
# MAGIC  select 
# MAGIC    ref_desc as channel,
# MAGIC    ref_code
# MAGIC  from dim_coded_consolidated where ref_type='CHANNEL'
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
# MAGIC )
# MAGIC select 
# MAGIC nrm.distributor_code,
# MAGIC nrm.site_code,
# MAGIC nrm.outlet_code,
# MAGIC nrm.product_code,
# MAGIC nrm.channel,
# MAGIC nvl(dt.dt_region, 'NA') as region,
# MAGIC nrm.standard_product_group_code,
# MAGIC nrm.banded,
# MAGIC nrm.promotion_type,
# MAGIC nrm.promotion_mechanism,
# MAGIC nrm.promotion_id,
# MAGIC nrm.budget_holder_group,
# MAGIC nrm.invoice_type,
# MAGIC nvl(case when nrm.banded ='Banded' then 1 else 0 end,0) banded_code,
# MAGIC nvl(case when ol.perfect_store = 'Y' then 1 else 0 end,0) perfect_store_status_code,
# MAGIC nvl(chn.ref_code,0) as channel_code,
# MAGIC 0 region_code,
# MAGIC nvl(store.store_size_code,0) as store_size_code,
# MAGIC nrm.gift_price_available,
# MAGIC sum(promotion_value) as promotion_value,
# MAGIC 0 tot_occdis,
# MAGIC 0 tot_perstr,
# MAGIC 0 tot_display,
# MAGIC 0 tot_ltyprg,
# MAGIC 0 has_occdis,
# MAGIC 0 has_perstr,
# MAGIC 0 has_occdis_and_perstr,
# MAGIC 0 has_occdis_or_perstr,
# MAGIC nrm.year_id,
# MAGIC nrm.month_id,
# MAGIC date_format(last_day(to_date (cast(time_key as string),'yyyyMMdd')),'yyyyMMdd') time_key
# MAGIC from fct_daily_promotions nrm
# MAGIC left outer join dim_outlet ol on nrm.outlet_code = ol.outlet_code
# MAGIC left outer join dim_distributor dt on nrm.site_code = dt.site_code
# MAGIC left outer join get_channel_code chn on nrm.channel= chn.channel
# MAGIC left outer join get_store_size_code store on nrm.outlet_code = store.outlet_code
# MAGIC where nrm.invoice_type='ON'
# MAGIC group by
# MAGIC nrm.distributor_code,nrm.site_code,nrm.outlet_code,nrm.product_code,nrm.channel,nvl(dt.dt_region, 'NA'),nrm.standard_product_group_code,nrm.banded,
# MAGIC nrm.promotion_type,nrm.promotion_mechanism,nrm.promotion_id,nrm.budget_holder_group,nrm.invoice_type,
# MAGIC nvl(case when nrm.banded ='Banded' then 1 else 0 end,0), nvl(case when ol.perfect_store = 'Y' then 1 else 0 end,0),
# MAGIC nvl(chn.ref_code,0), nvl(store.store_size_code,0), nrm.gift_price_available, nrm.year_id, nrm.month_id,
# MAGIC date_format(last_day(to_date (cast(time_key as string),'yyyyMMdd')),'yyyyMMdd');

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists fact_monthly_promotions_off;
# MAGIC create temporary view fact_monthly_promotions_off as
# MAGIC with get_channel_code (
# MAGIC  select 
# MAGIC    ref_desc as channel,
# MAGIC    ref_code
# MAGIC  from dim_coded_consolidated where ref_type='CHANNEL'
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
# MAGIC )
# MAGIC select 
# MAGIC nrm.*,
# MAGIC row_number() over (partition by outlet_code, product_code order by promotion_type) RN
# MAGIC from 
# MAGIC (select 
# MAGIC   nrm.distributor_code,
# MAGIC   nrm.site_code,
# MAGIC   nrm.outlet_code,
# MAGIC   nrm.product_code,
# MAGIC   nrm.channel,
# MAGIC   nvl(dt.dt_region, 'NA') as region,
# MAGIC   nrm.standard_product_group_code,
# MAGIC   nrm.banded,
# MAGIC   nrm.promotion_type,
# MAGIC   nrm.promotion_mechanism,
# MAGIC   nrm.promotion_id,
# MAGIC   nrm.budget_holder_group,
# MAGIC   nrm.invoice_type,
# MAGIC   nvl(case when nrm.banded ='Banded' then 1 else 0 end,0) banded_code,
# MAGIC   nvl(case when ol.perfect_store = 'Y' then 1 else 0 end,0) perfect_store_status_code,
# MAGIC   nvl(chn.ref_code,0) as channel_code,
# MAGIC   0 region_code,
# MAGIC   nvl(store.store_size_code,0) as store_size_code,
# MAGIC   nrm.gift_price_available,
# MAGIC   sum(promotion_value) as promotion_value,
# MAGIC   0 tot_occdis,
# MAGIC   0 tot_perstr,
# MAGIC   0 tot_display,
# MAGIC   0 tot_ltyprg,
# MAGIC   0 has_occdis,
# MAGIC   0 has_perstr,
# MAGIC   0 has_occdis_and_perstr,
# MAGIC   0 has_occdis_or_perstr,
# MAGIC   nrm.year_id,
# MAGIC   nrm.month_id,
# MAGIC   date_format(last_day(to_date (cast(time_key as string),'yyyyMMdd')),'yyyyMMdd') time_key
# MAGIC   from fct_daily_promotions nrm
# MAGIC   left outer join dim_outlet ol on nrm.outlet_code = ol.outlet_code
# MAGIC   left outer join dim_distributor dt on nrm.site_code = dt.site_code
# MAGIC   left outer join get_channel_code chn on nrm.channel= chn.channel
# MAGIC   left outer join get_store_size_code store on nrm.outlet_code = store.outlet_code
# MAGIC   where nrm.invoice_type='OFF'
# MAGIC   group by
# MAGIC   nrm.distributor_code,nrm.site_code,nrm.outlet_code,nrm.product_code,nrm.channel,nvl(dt.dt_region, 'NA'),nrm.standard_product_group_code,
# MAGIC   nrm.banded,nrm.promotion_type,nrm.promotion_mechanism,nrm.promotion_id,nrm.budget_holder_group,nrm.invoice_type,
# MAGIC   nvl(case when nrm.banded ='Banded' then 1 else 0 end,0), nvl(case when ol.perfect_store = 'Y' then 1 else 0 end,0),
# MAGIC   nvl(chn.ref_code,0), nvl(store.store_size_code,0), nrm.gift_price_available, nrm.year_id, nrm.month_id, 
# MAGIC   date_format(last_day(to_date (cast(time_key as string),'yyyyMMdd')),'yyyyMMdd')
# MAGIC ) nrm;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists fact_monthly_promotions_extra;
# MAGIC create temporary view fact_monthly_promotions_extra as
# MAGIC select 
# MAGIC distributor_code,
# MAGIC outlet_code,
# MAGIC product_code,
# MAGIC tot_occdis,
# MAGIC tot_perstr,
# MAGIC tot_display,
# MAGIC tot_ltyprg,
# MAGIC case when tot_occdis > 0 then 1 else 0 end has_occdis,
# MAGIC case when tot_perstr > 0 then 1 else 0 end has_perstr,
# MAGIC case when (tot_occdis > 0 and tot_perstr > 0) then 1 else 0 end has_occdis_and_perstr,
# MAGIC case when (tot_occdis > 0 or tot_perstr > 0) then 1 else 0 end has_occdis_or_perstr
# MAGIC from 
# MAGIC (select 
# MAGIC     nrm.distributor_code,
# MAGIC     nrm.outlet_code,
# MAGIC     nrm.product_code,
# MAGIC     sum(case when nrm.promotion_type = 'OCCDIS' then nrm.promotion_value else 0 end) tot_occdis,
# MAGIC     sum(case when nrm.promotion_type = 'PERSTR' then nrm.promotion_value else 0 end) tot_perstr,
# MAGIC     sum(case when nrm.promotion_type in ('OCCDIS','PERSTR') then nrm.promotion_value else 0 end) tot_display,
# MAGIC     sum(case when nrm.promotion_type = 'LTYPRG' then nrm.promotion_value else 0 end) tot_ltyprg
# MAGIC   from fct_daily_promotions nrm
# MAGIC   inner join dim_outlet ol on nrm.outlet_code = ol.outlet_code
# MAGIC   where invoice_type ='OFF'
# MAGIC   group by nrm.distributor_code, nrm.outlet_code, nrm.product_code
# MAGIC );

# COMMAND ----------

promoMthlyAggDF= spark.sql ("""
                            select 
                              distributor_code,
                              site_code,
                              outlet_code,
                              nvl(product_code,'NA') product_code,
                              channel,
                              region,
                              nvl(standard_product_group_code,'NA') standard_product_group_code,
                              banded,
                              promotion_type,
                              promotion_mechanism,
                              promotion_id,
                              budget_holder_group,
                              invoice_type,
                              cast(banded_code as int) banded_code,
                              cast(perfect_store_status_code as int) perfect_store_status_code,
                              cast(channel_code as int) channel_code,
                              cast(region_code as int) region_code,
                              store_size_code,
                              gift_price_available,
                              cast(promotion_value as double) promotion_value,
                              cast(tot_occdis as double) tot_occdis,
                              cast(tot_perstr as double) tot_perstr,
                              cast(tot_display as double) tot_display,
                              cast(tot_ltyprg as double) tot_ltyprg,
                              cast(has_occdis as int) has_occdis,
                              cast(has_perstr as int) has_perstr,
                              cast(has_occdis_and_perstr as int) has_occdis_and_perstr,
                              cast(has_occdis_or_perstr as int) has_occdis_or_perstr,
                              cast(year_id as int) year_id,
                              cast(month_id as int) month_id,
                              cast(time_key as int) time_key
                            from fact_monthly_promotions_on
                            union all
                            select 
                              off.distributor_code,
                              off.site_code,
                              off.outlet_code,
                              nvl(off.product_code,'NA') product_code,
                              off.channel,
                              off.region,
                              nvl(off.standard_product_group_code,'NA') standard_product_group_code,
                              off.banded,
                              off.promotion_type,
                              off.promotion_mechanism,
                              off.promotion_id,
                              off.budget_holder_group,
                              off.invoice_type,
                              cast(off.banded_code as int) banded_code,
                              cast(off.perfect_store_status_code as int) perfect_store_status_code,
                              cast(off.channel_code as int) channel_code,
                              cast(off.region_code as int) region_code,
                              cast(off.store_size_code as int) store_size_code,
                              off.gift_price_available,
                              cast(off.promotion_value as double) promotion_value,
                              cast(ext.tot_occdis as double) tot_occdis,
                              cast(ext.tot_perstr as double) tot_perstr,
                              cast(ext.tot_display as double) tot_display,
                              cast(ext.tot_ltyprg as double) tot_ltyprg,
                              cast(ext.has_occdis as int) has_occdis,
                              cast(ext.has_perstr as int) has_perstr,
                              cast(ext.has_occdis_and_perstr as int) has_occdis_and_perstr,
                              cast(ext.has_occdis_or_perstr as int) has_occdis_or_perstr,
                              cast(off.year_id as int) year_id,
                              cast(off.month_id as int) month_id,
                              cast(off.time_key as int) time_key
                            from fact_monthly_promotions_off off
                            left outer join fact_monthly_promotions_extra ext
                            on off.outlet_code = ext.outlet_code
                            and nvl(off.product_code, 'NA') = ext.product_code
                            and off.RN =1
                            and off.promotion_mechanism not in ('JR','NU')  
                            """)
promoMthlyAggDF.createOrReplaceTempView("fact_mthly_promotions")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists edge.fact_monthly_promo_agg;
# MAGIC create table if not exists edge.fact_monthly_promo_agg
# MAGIC (
# MAGIC distributor_code string,
# MAGIC site_code string,
# MAGIC outlet_code string,
# MAGIC product_code string,
# MAGIC channel string,
# MAGIC region string,
# MAGIC standard_product_group_code string,
# MAGIC banded string,
# MAGIC promotion_type string,
# MAGIC promotion_mechanism string,
# MAGIC promotion_id string,
# MAGIC budget_holder_group string,
# MAGIC invoice_type string,
# MAGIC banded_code integer,
# MAGIC perfect_store_status_code integer,
# MAGIC channel_code integer,
# MAGIC region_code integer,
# MAGIC store_size_code string,
# MAGIC gift_price_available string,
# MAGIC promotion_value double,
# MAGIC tot_occdis double,
# MAGIC tot_perstr double,
# MAGIC tot_display double,
# MAGIC tot_ltyprg double,
# MAGIC has_occdis int,
# MAGIC has_perstr int,
# MAGIC has_occdis_and_perstr int,
# MAGIC has_occdis_or_perstr int,
# MAGIC year_id int,
# MAGIC month_id int,
# MAGIC time_key int
# MAGIC )
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (year_id, month_id)
# MAGIC LOCATION "/mnt/adls/TTS/transformed/aggregates/fact_monthly_promo_agg";

# COMMAND ----------

# CHECK IF THE DATA AVAILABLE FOR PROCESSING MONTH THEN DELETE AND RELOAD

# CHECK IF DATA RELOAD HAPPENING OR NOT
reload_df = spark.sql("select * from edge.fact_monthly_promo_agg where year_id={} and month_id={}".format(yearID, monthID))

if (reload_df.count() > 0):
  print("Reload for month ID: {}. Deleting existing data before load".format(monthID))
  spark.sql ("""
             delete
              from edge.fact_monthly_promo_agg
              where year_id={} and month_id ={}
              """.format(yearID, monthID)
            )
else:
  print("Fresh Load for month ID: {}".format(monthID))

# DATA LOAD FOR DELTA NRM TABLE
print("Loading NRM transaction daily aggregate")
(
promoMthlyAggDF.repartition(10)
               .write
               .format("delta")
               .mode("append")
               .partitionBy("year_id", "month_id")
               .save(deltaPathMonthlyPromo)
)

print("Load completed for NRM transaction daily aggregate")

# (
# promoMthlyAggDF.repartition(10)
#                .write
#                .option("dataChange", "false")
#                .format("delta")
#                .mode("overwrite")
#                .partitionBy("year_id", "month_id")
#                .save(deltaPathMonthlyPromo)
# )

# COMMAND ----------

# MAGIC %md ##### post aggregate load validation

# COMMAND ----------

# MAGIC %run ./NRMDSS_PreCubeLoadValidation

# COMMAND ----------

(spark.sql("select * from edge.fact_monthly_promo_agg where month_id={}".format(monthID))
      .repartition(30)
      .write
      .mode("overwrite")
      .parquet (nrmMonthlyTempPath)
)

# REMOVING NON PART FILES BEFORE INITIATING COPY ACTIVITY USING POLYBASE
fileInfo= dbutils.fs.ls(nrmMonthlyTempPath)
fileList= [str(i.name) for i in fileInfo]

def filterNonPartFiles (fileName):
  if 'part' in fileName:
    return False
  else:
    return True

nonPartFiles = list(filter(filterNonPartFiles, fileList))

for file in nonPartFiles:
  dbutils.fs.rm(nrmMonthlyTempPath+"/{}".format(file))

# COMMAND ----------

# CHECK QUERIES
#
# %sql
# select promo.promo_invoice, count(1)
# from edge.processed_nrm_data nrm
# inner join 
# (select 
#   promo_invoice,
#   case when promotion_type ='BASKET' and promo_invoice='OFF' then 'BASKET_BD'
#        else promotion_type 
#   end as promotion_type
#   from dim_promotion
# ) promo on nrm.promotion_type = promo.promotion_type
# where nrm.month_id=202001
# group by promo.promo_invoice;

# select * from dim_distributor 
# where (dt_region is not null or dt_area is not null or dt_district is not null);

# select * from fact_monthly_promotions_off where outlet_code='0000346820';
