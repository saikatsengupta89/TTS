# Databricks notebook source
from pyspark.sql.functions import col

def getQuarter(month):
  if month in ['01','02','03']:
    return 'Quarter 1'
  elif month in ['04','05','06']:
    return 'Quarter 2'
  elif month in ['07','08','09']:
    return 'Quarter 3'
  else:
    return 'Quarter 4'

processingYear  =dbutils.widgets.get("ProcessingYear")
processingMonth =dbutils.widgets.get("ProcessingMonth")
yearMonth= int(processingYear+processingMonth)
quarter= getQuarter(processingMonth)

# COMMAND ----------

pathMappingPromo       = "/mnt/adls/TTS/transformed/dimensions/dim_promotion"
pathMappingIOMaster    = "/mnt/adls/TTS/transformed/facts/ref_mthly_io"
pathGiftMaster         = "/mnt/adls/TTS/processed/references/ref_gift_master/{}/{}".format(processingYear, processingMonth)
pathGiftPriceMaster    = "/mnt/adls/TTS/processed/references/ref_gift_price_master/{}/{}".format(processingYear, processingMonth)

# COMMAND ----------

mappingPromoDF= spark.read.parquet(pathMappingPromo)
mappingPromoDF.createOrReplaceTempView("dim_promotion")

mapIOMasterDF= spark.read.parquet(pathMappingIOMaster+"/"+processingYear+"/"+processingMonth)
mapIOMasterDF.createOrReplaceTempView("ref_mthly_io")

mapGiftMasterDF= spark.read.parquet(pathGiftMaster)
mapGiftMasterDF.createOrReplaceTempView("ref_gift_master")

mapGiftPriceMasterDF= spark.read.parquet(pathGiftPriceMaster)
mapGiftPriceMasterDF.createOrReplaceTempView("ref_gift_price_master")

# COMMAND ----------

exception_df = spark.sql("""
                          select *
                          from
                          (select 
                            case when promotion_type in ('BASKET','LTYPRG') then 1 else 0 end promo_flag, 
                            case when promotion_mechanism ='FQ' then 1 else 0 end as promo_mec_flag
                            from edge.processed_nrm_data where year_id={} and month_id={}
                            and on_off_flag=0 
                            and promotion_mechanism not in ('JR','NU')
                          ) 
                          where (promo_flag =0 and promo_mec_flag=0)
                          """.format(processingYear, yearMonth)
                        )
try:
  if (exception_df.count() > 0):
    raise Exception ("There are promotions or promotion mechanism available in Non UL data whcih are neither BASKET/LTYPRG or FQ/JR")
except Exception as e:
  raise dbutils.notebook.exit(e)

# COMMAND ----------

# MAGIC %run ./GET_StandardProductHierarchy

# COMMAND ----------

nonULGiftDataDFT1 = spark.sql("""
                              select 
                              nrm.*,
                              substring(nrm.promotion_id, 3, 8) scheme_id,
                              io.brand_code,
                              io.standard_product_group_code,
                              nvl(io.category, 'NA') as category_IO,
                              nvl(io.brand_code, 'NA') as brand_IO,
                              nvl(io.budget_holder_grp, 'Trade Category') as budget_holder_group,
                              nvl(proportion, 0) as proportion
                              from 
                              edge.processed_nrm_data nrm
                              left outer join
                              (select 
                                io.*,
                                ph.standard_product_group_code,
                                ph.master_product_category,
                                ph.master_market
                                from ref_mthly_io io
                                left join 
                                (select 
                                  distinct
                                  brand_code,
                                  standard_product_group_code,
                                  master_product_category,
                                  master_market
                                  from ref_standard_hierarchy
                                ) ph on io.brand_code = ph.brand_code
                              ) io on nrm.IO= io.IO
                              where year_id={} and month_id={}
                              and on_off_flag=0
                              and promotion_mechanism not in ('JR','NU')
                              """.format(processingYear, yearMonth)
                             )
nonULGiftDataDFT1.createOrReplaceTempView("non_ul_gift_data_t1")

# COMMAND ----------

giftPriceMasterByItemSchemeDF = spark.sql("""
                                          select 
                                            scheme_id,
                                            item_code,
                                            max(price_vnd) as price_vnd
                                            from ref_gift_price_master
                                            group by scheme_id, item_code
                                          """)
giftPriceMasterByItemSchemeDF.createOrReplaceTempView("gift_price_master_by_item_scheme")

giftPriceMasterByItemDF = spark.sql("""
                                    select 
                                      item_code,
                                      max(price_vnd) as price_item
                                      from ref_gift_price_master
                                      group by item_code
                                    """)
giftPriceMasterByItemDF.createOrReplaceTempView("gift_price_master_by_item")

# COMMAND ----------

nonULGiftDataDF = spark.sql ("""
                             select
                                nrm.invoice_no,
                                nrm.product_code,
                                nrm.standard_product_group_code,
                                nrm.promotion_id,
                                nrm.IO,
                                nrm.promotion_mechanism,
                                nrm.promotion_desc,
                                nrm.promotion_type,
                                nrm.promo_start_date,
                                nrm.promo_end_date,
                                nrm.free_qty_in_cs,
                                nrm.free_qty_in_pc,
                                nrm.value_based_promo_disc,
                                nrm.header_lvl_disc,
                                nrm.free_quantity_value,
                                nrm.promotion_value,
                                nrm.distributor_code,
                                nrm.site_code,
                                nrm.outlet_code,                                         
                                nrm.country,
                                nrm.year_id,
                                nrm.month_id,
                                nrm.time_key,
                                budget_holder_group,
                                nvl(proportion, 0) as proportion,
                                nvl(nvl(gftis.price_vnd, gfti.price_item),0) retail_price_pcs,
                                case when nvl(gftis.price_vnd, gfti.price_item) is not null then 'Y' else 'N' end gift_price_available,
                                nvl(gm.packing_per_case,0) as packing_per_case
                              from non_ul_gift_data_t1 nrm
                              left outer join gift_price_master_by_item_scheme gftis
                              on nrm.product_code = gftis.item_code and nrm.scheme_id= gftis.scheme_id
                              left outer join gift_price_master_by_item gfti
                              on nrm.product_code = gfti.item_code
                              left outer join 
                              (select 
                                 gift_code, 
                                 packing_per_case
                                from ref_gift_master
                              ) gm on nrm.product_code = gm.gift_code
                             """)
nonULGiftDataDF.createOrReplaceTempView("non_ul_gift_data")

# COMMAND ----------

nonULGiftData = spark.sql("""
                          select 
                            nrm.invoice_no,
                            nrm.product_code,
                            nrm.standard_product_group_code,
                            nrm.promotion_id,
                            nrm.IO,
                            nrm.promotion_mechanism,
                            nrm.promotion_desc,
                            nrm.promotion_type,
                            budget_holder_group,
                            'OFF' invoice_type,
                            nrm.promo_start_date,
                            nrm.promo_end_date,
                            nrm.value_based_promo_disc,
                            nrm.header_lvl_disc,
                            nrm.free_quantity_value,
                            (promotion_value_vnd * nvl(proportion,0)) promotion_value,
                            nrm.distributor_code,
                            nrm.site_code,
                            nrm.outlet_code,
                            'Non-Banded' banded,
                            gift_price_available,
                            nrm.country,
                            nrm.year_id,
                            nrm.month_id,
                            nrm.time_key
                          from
                          (select 
                            nrm.*,
                            ((nvl(free_qty_in_cs,0) * nvl(packing_per_case,0) * nvl(retail_price_pcs,0)) +
                             (nvl(free_qty_in_pc,0) * nvl(retail_price_pcs,0))
                            ) promotion_value_vnd
                            from non_ul_gift_data nrm
                          ) nrm
                          """)
