# Databricks notebook source
yearID= int(dbutils.widgets.get("ProcessingYear"))
monthID= int(str(yearID) +dbutils.widgets.get("ProcessingMonth"))

# COMMAND ----------

deltaPathDailyPromotions="/mnt/adls/TTS/transformed/facts/fact_daily_promotions"
pathDimChannel= "/mnt/adls/TTS/transformed/dimensions/dim_channel"
pathDimOutlet= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Outlet/"
nrmDailyTempPath="/mnt/adls/TTS/temp/temp_fact_daily_promotions"

# COMMAND ----------

dimOutletDF= spark.read.option("header","true") \
                         .option("inferSchema","true") \
                         .csv(pathDimOutlet)

dimChannelDF = spark.read.parquet(pathDimChannel)

dimOutletDF.createOrReplaceTempView ("dim_outlet")
dimChannelDF.createOrReplaceTempView("dim_channel")

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table edge.processed_nrm_data;

# COMMAND ----------

# MAGIC %md ##### call Final Aggregate Notebook

# COMMAND ----------

# MAGIC %run ./NRMDSS_DailyAggregate

# COMMAND ----------

# MAGIC %md ##### create DELTA table on top of transformed NRM data

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists edge.fact_daily_promotions;
# MAGIC create table if not exists edge.fact_daily_promotions
# MAGIC (
# MAGIC invoice_number string,
# MAGIC product_code string,
# MAGIC standard_product_group_code string,
# MAGIC promotion_id string,
# MAGIC IO string,
# MAGIC promotion_mechanism string,
# MAGIC promotion_desc string,
# MAGIC promotion_type string,
# MAGIC budget_holder_group string,
# MAGIC invoice_type string,
# MAGIC promo_start_date date,
# MAGIC promo_end_date date,
# MAGIC value_based_promo_disc double,
# MAGIC header_lvl_disc double,
# MAGIC free_quantity_value double,
# MAGIC promotion_value double,
# MAGIC distributor_code string,
# MAGIC site_code string,
# MAGIC outlet_code string,
# MAGIC channel string,
# MAGIC banded string,
# MAGIC gift_price_available string,
# MAGIC country string,
# MAGIC year_id int,
# MAGIC month_id int,
# MAGIC time_key int
# MAGIC )
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (year_id, month_id, time_key)
# MAGIC LOCATION "/mnt/adls/TTS/transformed/facts/fact_daily_promotions";
# MAGIC 
# MAGIC ALTER TABLE edge.fact_daily_promotions
# MAGIC SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

# COMMAND ----------

transformed_nrm_data.createOrReplaceTempView("final_nrm_data")
nrmDeltaDF= spark.sql("""select 
                          cast(invoice_no as string) as invoice_number,
                          cast(product_code as string) as product_code,
                          cast(standard_product_group_code as string) as standard_product_group_code,
                          cast(promotion_id as string) as promotion_id,
                          cast(IO as string) as IO,
                          cast(promotion_mechanism as string) as promotion_mechanism,
                          cast(promotion_desc as string) as promotion_desc,
                          cast(promotion_type as string) as promotion_type,
                          cast(budget_holder_group as string) as budget_holder_group,
                          cast(invoice_type as string) as invoice_type,
                          to_date(promo_start_date, 'dd.MM.yyyy') as promo_start_date,
                          to_date(promo_end_date, 'dd.MM.yyyy') as promo_end_date,
                          cast(value_based_promo_disc as double) as value_based_promo_disc,
                          cast(header_lvl_disc as double) as header_lvl_disc,
                          cast(free_quantity_value as double) as free_quantity_value,
                          cast(promotion_value as double) as promotion_value,
                          cast(distributor_code as string) as distributor_code,
                          cast(site_code as string) as site_code,
                          cast(nrm.outlet_code as string) as outlet_code,
                          cast(ol.channel as string) as channel,
                          cast(banded as string) as banded,
                          cast(gift_price_available as string) as gift_price_available,
                          cast(country as string) as country,
                          cast(year_id as int) as year_id,
                          cast(month_id as int) as month_id,
                          cast(time_key as int) as time_key
                         from final_nrm_data nrm
                         left outer join 
                         (select
                            distinct
                            ol.outlet_code,
                            case when upper(chn.group_channel) ='PROFESSIONAL' then 'PROFESSIONAL'
                                 else chn.channel
                                 end channel
                          from dim_outlet ol
                          inner join dim_channel chn on chn.channel= ol.channel
                         ) ol on nrm.outlet_code = ol.outlet_code
                     """)
#11080110
#12495230

# COMMAND ----------

# CHECK IF THE DATA AVAILABLE FOR PROCESSING MONTH THEN DELETE AND RELOAD

# CHECK IF DATA RELOAD HAPPENING OR NOT
reload_df = spark.sql("select * from edge.fact_daily_promotions where month_id={}".format(monthID))

if (reload_df.count() > 0):
  print("Reload for month ID: {}. Deleting existing data before load".format(monthID))
  spark.sql ("""
             delete
              from edge.fact_daily_promotions
              where month_id ={}
              """.format(monthID)
            )
else:
  print("Fresh Load for month ID: {}".format(monthID))

# DATA LOAD FOR DELTA NRM TABLE
print("Loading NRM transaction daily aggregate")
(
nrmDeltaDF.write
          .format("delta")
          .mode("append")
          .partitionBy("year_id", "month_id", "time_key")
          .save(deltaPathDailyPromotions)
)

print("Load completed for NRM transaction daily aggregate")

# COMMAND ----------

# MAGIC %sql
# MAGIC analyze table edge.fact_daily_promotions compute statistics;
# MAGIC optimize edge.fact_daily_promotions;

# COMMAND ----------

# MAGIC %md ##### post transform validation

# COMMAND ----------

# MAGIC %run ./NRMDSS_PostTransformValidation

# COMMAND ----------

# MAGIC %md ##### incremental write to TEMP before pushing to SQLDW

# COMMAND ----------

(spark.sql("select * from edge.fact_daily_promotions where year_id={} and month_id={}".format(yearID, monthID))
      .repartition(30)
      .write
      .mode("overwrite")
      .parquet (nrmDailyTempPath)
)

# REMOVING NON PART FILES BEFORE INITIATING COPY ACTIVITY USING POLYBASE
fileInfo= dbutils.fs.ls(nrmDailyTempPath)
fileList= [str(i.name) for i in fileInfo]

def filterNonPartFiles (fileName):
  if 'part' in fileName:
    return False
  else:
    return True

nonPartFiles = list(filter(filterNonPartFiles, fileList))

for file in nonPartFiles:
  dbutils.fs.rm(nrmDailyTempPath+"/{}".format(file))
