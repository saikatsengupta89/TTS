# Databricks notebook source
mappingPromo = "/mnt/adls/TTS/transformed/dimensions/dim_promotion"
mappingIOMaster= "/mnt/adls/TTS/transformed/facts/ref_mthly_io"
mappingBudgetHolder= "/mnt/adls/TTS/processed/masters/mapping_budget_holder"
# dbutils.widgets.text("ProcessingYear","2019")
# dbutils.widgets.text("ProcessingMonth", "01")

processingYear  =dbutils.widgets.get("ProcessingYear")
processingMonth =dbutils.widgets.get("ProcessingMonth")
year_id = processingYear
month_id= int(processingYear+processingMonth)

# COMMAND ----------

yearIO = processingYear
monthIO= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (mappingIOMaster +"/" +str(yearIO))])).zfill(2)

mappingPromoDF= spark.read.parquet(mappingPromo)
mappingPromoDF.createOrReplaceTempView("dim_promotion")

mapBudgetHolderDF= spark.read.parquet(mappingBudgetHolder)
mapBudgetHolderDF.createOrReplaceTempView("ref_budget_holder")

mapIOMasterDF= spark.read.parquet(mappingIOMaster+"/"+yearIO+"/"+monthIO)
mapIOMasterDF.createOrReplaceTempView("ref_mthly_io")

# COMMAND ----------

check_extra_promo= spark.sql("""
                              select 
                                distinct nrm.promotion_type
                              from edge.processed_nrm_data nrm
                              left outer join (select distinct promotion_type from dim_promotion) promo 
                              on promo.promotion_type=case when nrm.promotion_type='BASKET_BD' then 'BASKET' else nrm.promotion_type end
                              where year_id={} and month_id = {}
                              and promo.promotion_type is null
                              """.format(year_id, month_id))
try:
  if (check_extra_promo.count() > 0):
    raise Exception ("There are new promotions available in NRM data not present in promo mapping file")
except Exception as e:
  raise dbutils.notebook.exit(e)

# COMMAND ----------

# MAGIC %md ##### call standard customized product hierarchy

# COMMAND ----------

# MAGIC %run ./GET_StandardProductHierarchy

# COMMAND ----------

# MAGIC %md ##### create standardize ON Invoice dataframe

# COMMAND ----------

nrmOnInvoiceData = spark.sql ("""
                              select 
                                nrm.invoice_no,
                                nrm.product_code,
                                sph.standard_product_group_code,
                                substring(nrm.promotion_id,1,10) as IO,
                                nrm.promotion_id,
                                nrm.promotion_mechanism,
                                nrm.promotion_desc,
                                nrm.promotion_type,
                                promo.promo_invoice as invoice_type,
                                nrm.promo_start_date,
                                nrm.promo_end_date,
                                nrm.value_based_promo_disc,
                                nrm.header_lvl_disc,
                                nrm.free_quantity_value,
                                nrm.promotion_value,
                                nrm.distributor_code,
                                nrm.site_code,
                                nrm.outlet_code,
                                sph.banded,
                                nrm.country,
                                nrm.year_id,
                                nrm.month_id,
                                nrm.time_key
                              from edge.processed_nrm_data nrm
                              inner join (
                              select 
                              promo_invoice,
                              case when promotion_type ='BASKET' and promo_invoice='OFF' then 'BASKET_BD'
                                   else promotion_type 
                              end as promotion_type
                              from dim_promotion
                              ) promo on nrm.promotion_type = promo.promotion_type
                              inner join ref_standard_hierarchy sph on nrm.product_code = sph.product_code
                              where year_id = {} and month_id = {}
                              and promo.promo_invoice='ON' and nrm.on_off_flag =1
                              """.format(year_id, month_id))

nrmOnInvoiceData.createOrReplaceTempView('on_invoice_promo')

# COMMAND ----------

# CHECK SQL QUERIES
# %sql
# select promotion_type, count(1)
# from edge.processed_nrm_data 
# where year_id=2019 and month_id=201901
# --and on_off_flag=1
# group by promotion_type;

# %sql
# select 
# nrm.product_code,
# dp.product_code dp_prod_code
# from edge.processed_nrm_data nrm
# left join ref_dp_ph_hierarchy dp
# on nrm.product_code= dp.product_code
# where dp.product_code is null
# group by nrm.product_code, dp.product_code

# COMMAND ----------

nrmOnInvoiceData=spark.sql("""select 
                                invoice_no,
                                product_code,
                                standard_product_group_code,
                                promotion_id,
                                IO,
                                promotion_mechanism,
                                promotion_desc,
                                promotion_type,
                                (case
                                  when budget_holder_grp is null and promotion_type='DTRDIS' then 'Distributor'
                                  when budget_holder_grp is null then 'Trade Category'
                                  else budget_holder_grp
                                end) as budget_holder_group,
                                invoice_type,
                                promo_start_date,
                                promo_end_date,
                                value_based_promo_disc,
                                header_lvl_disc,
                                free_quantity_value,
                                promotion_value,
                                distributor_code,
                                site_code,
                                outlet_code,
                                banded,
                                country,
                                year_id,
                                month_id,
                                time_key
                              from 
                              (select a.*, b.budget_holder_grp from on_invoice_promo as a
                              left join 
                              (select distinct IO,budget_holder_grp
                              from ref_mthly_io 
                              ) as b
                              on a.IO=b.IO)""")
