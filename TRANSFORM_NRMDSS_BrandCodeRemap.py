# Databricks notebook source
import pandas as pd
from pyspark.sql import types
from pyspark.sql.functions import col

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

processingYear  =dbutils.widgets.get("ProcessingYear")
processingMonth =dbutils.widgets.get("ProcessingMonth")

# COMMAND ----------

pathPRCIOMaster='/mnt/adls/TTS/processed/references/ref_mthly_io'
pathPRCBudgetHolder='/mnt/adls/TTS/processed/masters/mapping_budget_holder'
localPHStg   = "/mnt/adls/EDGE_Analytics/Datalake/Staging/Incremental/Masters/Product_Hierarchy_Mapping"
pathTRANIOMaster='/mnt/adls/TTS/transformed/facts/ref_mthly_io'
pathBrandCodeMap="/mnt/adls/TTS/processed/references/ref_old_new_brand_code"

#reading the latest article master from the raw layer in adls
yearIOM = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster)]))
monthIOM= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster +"/" +str(yearIOM))])).zfill(2)

# COMMAND ----------

brandCodeMapData= spark.read.parquet(pathBrandCodeMap)
brandCodeMapData.createOrReplaceTempView("ref_brand_code")

yearLPH = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(localPHStg)]))
monthLPH= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(localPHStg +"/" +str(yearLPH))])).zfill(2)
dayLPH  = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(localPHStg +"/"+str(yearLPH)+"/"+str(monthLPH))])).zfill(2)

localPHStgDF= spark.read \
                   .option("inferSchema",True) \
                   .option("header", True) \
                   .csv(localPHStg+"/"+yearLPH+"/"+monthLPH+"/"+dayLPH)
localPHStgDF.createOrReplaceTempView("stg_local_ph")

localPHDF= spark.sql ("""
                      select 
                       distinct
                        `PH7` as PH7_code,
                        `Small C` as category
                      from stg_local_ph
                      """)

localPHDF.createOrReplaceTempView("dim_local_product")

# COMMAND ----------

check_duplicate= spark.sql("select PH7_old, count(1) cnt from ref_brand_code group by PH7_old having count(1) > 1")
try:
  if (check_duplicate.count() > 0):
    raise Exception ("There are duplicate PH7 Old in old to new brand code mapping file. Stopping the process")
except Exception as e:
  raise dbutils.notebook.exit(e)

# COMMAND ----------

if (monthIOM=='01' or monthIOM=='02') :
  yearIOM_1 = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster)])-1)
  monthIOM_1= str(min([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster +"/" +str(yearIOM_1))])).zfill(2)
  #print(pathPRCIOMaster+'/'+yearIOM+'/'+monthIOM)
  
  IOMasterDF=(spark.read
           .format('parquet')
           .load(pathPRCIOMaster+'/'+yearIOM+'/'+monthIOM) #
           .select('promotion_id','budget_holder','spend_type','brand_code','brand_percentage','brand_investment','investment_amount')
           .distinct())

  IOMasterDF.createOrReplaceTempView('IOMaster_jan')
  #display(IOMasterDF)
  
  #print(pathPRCIOMaster+'/'+yearIOM_1+'/'+monthIOM_1)
  IOMasterDF_1=(spark.read
           .format('parquet')
           .load(pathPRCIOMaster+'/'+yearIOM_1+'/'+monthIOM_1) #
           .select('promotion_id','budget_holder','spend_type','brand_code','brand_percentage','brand_investment','investment_amount')
           .distinct())

  IOMasterDF_1.createOrReplaceTempView('IOMaster_dec')
  #display(IOMasterDF_1)
  
  
  spark.sql("""select promotion_id,budget_holder,spend_type,brand_code,brand_percentage,brand_investment,investment_amount
  from
  (select a.*,
  RANK() over (
  partition by promotion_id
  order by year desc ) as recent_promo
  from 
  (select promotion_id,budget_holder,spend_type,brand_code,brand_percentage,brand_investment,investment_amount,{} as year 
  from IOMaster_jan
  union
  select promotion_id,budget_holder,spend_type,brand_code,brand_percentage,brand_investment,investment_amount,{} as year
  from IOMaster_dec) as a) as b
  where recent_promo=1""".format(processingYear, (int(processingYear)-1))).createOrReplaceTempView('IOMaster')
else :
  IOMasterDF=(spark.read
           .format('parquet')
           .load(pathPRCIOMaster+'/'+yearIOM+'/'+monthIOM) #
           .select('promotion_id','budget_holder','spend_type','brand_code','brand_percentage','brand_investment','investment_amount')
           .distinct())

  IOMasterDF.createOrReplaceTempView('IOMaster')

# COMMAND ----------

BudgetHolderDF=(spark.read
              .format('parquet')
              .load(pathPRCBudgetHolder)
              .distinct())

BudgetHolderDF.createOrReplaceTempView('budget_holder')

# COMMAND ----------

IODFT1= spark.sql("""
                  select 
                    io.IO,
                    io.budget_holder,
                    io.budget_holder_grp,
                    io.brand_code,
                    lph.category,
                    io.brand_percentage,
                    io.brand_investment,
                    io.investment_amount 
                  from
                  (select
                      concat(a.spend_type,a.promotion_id) as IO,
                      a.promotion_id,
                      a.budget_holder,
                      b.budget_holder_grp,
                      a.spend_type,
                      nvl(c.PH7_latest, a.brand_code) brand_code,
                      a.brand_percentage,
                      a.brand_investment,
                      a.investment_amount 
                    from IOMaster a
                    left join budget_holder b on a.budget_holder=b.budget_holder
                    left outer join ref_brand_code c on a.brand_code = c.PH7_old
                  ) io 
                  left outer join dim_local_product lph on io.brand_code = lph.PH7_code
                  """)
IODFT1.createOrReplaceTempView("parent_view")

# COMMAND ----------

(spark.sql("""select IO,category,sum(brand_investment) as total_IO_amt_PERSTR from parent_view group by IO,category""")
 .createOrReplaceTempView('IO_sum_perstr'))
 
(spark.sql("""select IO,sum(brand_investment) as total_IO_amt from parent_view group by IO""")
 .createOrReplaceTempView('IO_sum'))

# COMMAND ----------

updatedIODF= spark.sql("""
                        select 
                          aa.IO,aa.budget_holder,aa.budget_holder_grp,aa.brand_code,nvl(aa.category,'NA') as category,
                          nvl(aa.brand_percentage,0) as brand_percentage,
                          nvl(aa.brand_investment,0) as brand_investment,
                          nvl(aa.investment_amount,0) as investment_amount,
                          nvl(aa.total_IO_amt_PERSTR,0) as total_IO_amt_PERSTR,
                          nvl(bb.total_IO_amt,0) as total_IO_amt,
                          nvl((aa.brand_investment/aa.total_IO_amt_PERSTR),0) as proportion_PERSTR,
                          nvl((aa.brand_investment/bb.total_IO_amt),0) as proportion from 
                        (select 
                          a.IO, 
                          a.budget_holder, a.budget_holder_grp,
                          a.brand_code,
                          a.category,
                          a.brand_percentage,
                          a.brand_investment,
                          a.investment_amount,
                          b.total_IO_amt_PERSTR 
                        from parent_view a 
                        left join IO_sum_perstr b on a.IO=b.IO and a.category=b.category
                        ) as  aa
                        left join IO_sum bb on aa.IO=bb.IO
                        """)

# COMMAND ----------

# updatedIODF= spark.sql("""
#                         select 
#                           io.IO,
#                           io.budget_holder,
#                           io.budget_holder_grp,
#                           io.brand_code as old_brand_code,
#                           io.new_brand_code as brand_code,
#                           io.category as category_old,
#                           case when nvl(io.category,'NA')='NA' then nvl(lph.category, 'NA') 
#                                else io.category end category,
#                           io.brand_percentage,
#                           io.brand_investment,
#                           io.investment_amount,
#                           io.total_IO_amt_PERSTR,
#                           io.total_IO_amt,
#                           case when io.total_IO_amt_PERSTR=0 then 1 else nvl((io.brand_investment/io.total_IO_amt_PERSTR),0) end proportion_PERSTR,
#                           nvl((io.brand_investment/io.total_IO_amt),0) as proportion
#                         from
#                         (select 
#                           io.*,
#                           nvl(bc.PH7_latest, io.brand_code) new_brand_code
#                           from ref_mthly_io io
#                           left outer join ref_brand_code bc on io.brand_code = bc.PH7_old
#                         ) io
#                         left outer join 
#                         (select distinct PH7_code, category from dim_local_product) lph 
#                         on lph.PH7_code= io.new_brand_code
#                        """)

# COMMAND ----------

# WRITING UPDATED IO FILE TO TRANSFORMED LAYER
(
updatedIODF.repartition(1)
               .write
               .mode("overwrite")
               .parquet(pathTRANIOMaster+"/"+processingYear+"/"+processingMonth)
)
