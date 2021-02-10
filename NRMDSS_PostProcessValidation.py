# Databricks notebook source
processingYear  =dbutils.widgets.get("ProcessingYear")
processingMonth =dbutils.widgets.get("ProcessingMonth")
processingMonthID= processingYear + processingMonth

# COMMAND ----------

def checkPathExists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    return False

# COMMAND ----------

mappingPromo = "/mnt/adls/TTS/transformed/dimensions/dim_promotion"
mappingIOMaster= "/mnt/adls/TTS/transformed/facts/ref_mthly_io"
pathExceptionMissingProductCode = f"/mnt/adls/TTS/processed/transactions/monthly/nrm_dss_quality_chck/{processingMonthID}/missing_product_from_PH"
pathExceptionMissingIO = f"/mnt/adls/TTS/processed/transactions/monthly/nrm_dss_quality_chck/{processingMonthID}/missing_IO_from_MECTTS"
pathExceptionMissingPH7 = f"/mnt/adls/TTS/processed/transactions/monthly/nrm_dss_quality_chck/{processingMonthID}/MECTTS_PH7_missing_from_AM_OR_LPH"

# COMMAND ----------

yearIO = processingYear
monthIO= processingMonth if (checkPathExists(mappingIOMaster+"/"+processingYear+"/"+processingMonth)) else  str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (mappingIOMaster +"/" +str(yearIO))])).zfill(2)

print(monthIO)

mappingPromoDF= spark.read.parquet(mappingPromo)
mappingPromoDF.createOrReplaceTempView("dim_promotion")

mapIOMasterDF= spark.read.parquet(mappingIOMaster+"/"+processingYear+"/"+processingMonth)
mapIOMasterDF.createOrReplaceTempView("ref_mthly_io")

# COMMAND ----------

# MAGIC %run ./GET_StandardProductHierarchy

# COMMAND ----------

exceptionMissingProductOnInvoiceDF= spark.sql ("""
                                               select 
                                                distinct 
                                                nrm.product_code as nrm_product_code, 
                                                ph.product_code as lph_product_code
                                                from edge.processed_nrm_data nrm
                                                inner join 
                                                (select 
                                                  promo_invoice,
                                                  case when promotion_type ='BASKET' and promo_invoice='OFF' then 'BASKET_BD'
                                                       else promotion_type 
                                                  end as promotion_type
                                                from dim_promotion
                                                ) promo on nrm.promotion_type = promo.promotion_type
                                                left outer join ref_standard_hierarchy ph on nrm.product_code = ph.product_code
                                                where nrm.year_id={}
                                                and nrm.month_id={}
                                                and promo.promo_invoice='ON'
                                                and ph.product_code is null
                                                """.format(processingYear, processingMonthID)
                                              )
if (exceptionMissingProductOnInvoiceDF.count() > 0):
  (exceptionMissingProductOnInvoiceDF.repartition(1)
                                     .write
                                     .mode("overwrite")
                                     .option("header","true")
                                     .csv(pathExceptionMissingProductCode)
  )

# COMMAND ----------

exceptionMissingIODF= spark.sql ("""
                                 select 
                                  distinct 
                                  nrm.IO as transactional_IO, 
                                  io.IO as MECTTS_IO, 
                                  promo.promo_invoice invoice_type
                                  from edge.processed_nrm_data nrm
                                  inner join 
                                  (select 
                                    promo_invoice,
                                    case when promotion_type ='BASKET' and promo_invoice='OFF' then 'BASKET_BD'
                                         else promotion_type 
                                    end as promotion_type
                                  from dim_promotion
                                  ) promo on nrm.promotion_type = promo.promotion_type
                                  left outer join ref_mthly_io io on nrm.IO = io.IO
                                  where nrm.year_id={}
                                  and nrm.month_id={}
                                  and nrm.promotion_mechanism not in ('JR','NU')
                                  and io.IO is null
                                  """.format(processingYear, processingMonthID)
                               )

if (exceptionMissingIODF.count() > 0):
  (exceptionMissingIODF.repartition(1)
                       .write
                       .mode("overwrite")
                       .option("header","true")
                       .csv(pathExceptionMissingIO)
  )

# COMMAND ----------

exceptionPH7MECTTSNotInLPHOrAM = spark.sql("""
                                           select *
                                            from 
                                            (select
                                              IO transactional_IO,
                                              brand_code transactional_brand_code,
                                              case when lph.PH7_code is not null then 'Y' else 'N' end brand_code_avail_LPH,
                                              case when am.PH7_code is not null then 'Y' else 'N' end brand_code_avail_AM
                                              from
                                              (select 
                                                distinct nrm.IO, io.brand_code
                                                from edge.processed_nrm_data nrm
                                                inner join 
                                                (select 
                                                  promo_invoice,
                                                  case when promotion_type ='BASKET' and promo_invoice='OFF' then 'BASKET_BD'
                                                       else promotion_type 
                                                  end as promotion_type
                                                from dim_promotion
                                                ) promo on nrm.promotion_type = promo.promotion_type
                                                inner join ref_mthly_io io on nrm.IO = io.IO
                                                where nrm.year_id={}
                                                and nrm.month_id={}
                                                and promo.promo_invoice='OFF'
                                                and nrm.promotion_mechanism not in ('JR','NU')
                                              ) io
                                              left outer join (select distinct PH7_code from ref_article_master) am on io.brand_code = am.PH7_code
                                              left outer join (select distinct PH7_code from dim_local_product) lph on io.brand_code = lph.PH7_code
                                            ) 
                                            where (brand_code_avail_LPH='N' or brand_code_avail_AM='N')
                                            """.format(processingYear, processingMonthID)
                                          )
if (exceptionPH7MECTTSNotInLPHOrAM.count() > 0):
  (exceptionPH7MECTTSNotInLPHOrAM.repartition(1)
                                 .write
                                 .mode("overwrite")
                                 .option("header","true")
                                 .csv(pathExceptionMissingPH7)
  )
