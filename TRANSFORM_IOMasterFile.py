# Databricks notebook source
import pandas as pd
from pyspark.sql import types
from pyspark.sql.functions import col
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled','false')

pathPRCIOMaster='dbfs:/mnt/adls/TTS/processed/references/ref_mthly_io'
pathPRCBudgetHolder='dbfs:/mnt/adls/TTS/processed/masters/mapping_budget_holder'
localPHStg   = "/mnt/adls/EDGE_Analytics/Datalake/Staging/Incremental/Masters/Product_Hierarchy_Mapping"
pathTRANIOMasterFile='dbfs:/mnt/adls/TTS/transformed/facts/ref_mthly_io'

processingYear  =dbutils.widgets.get("ProcessingYear")
processingMonth =dbutils.widgets.get("ProcessingMonth")

#reading the latest article master from the raw layer in adls
yearIOM = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster)]))
monthIOM= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster +"/" +str(yearIOM))])).zfill(2)
#yearIOM = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster)]))
# yearIOM_1=str(min([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster)]))
# monthIOM_1= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster +"/" +str(yearIOM_1))])).zfill(2)

# COMMAND ----------

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
                        `PH7` as PH7_code,
                        `Small C` as category
                      from stg_local_ph
                      """).distinct()

localPHDF.createOrReplaceTempView("dim_local_product")
#display(localPHDF)

# COMMAND ----------

if (monthIOM=='01' or monthIOM=='02') :
  yearIOM_1 = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster)])-1)
  monthIOM_1= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster +"/" +str(yearIOM_1))])).zfill(2)
  
  print(pathPRCIOMaster+'/'+yearIOM+'/'+monthIOM)
  IOMasterDF=(spark.read
           .format('parquet')
           .load(pathPRCIOMaster+'/'+yearIOM+'/'+monthIOM) #
           .select('promotion_id','budget_holder','spend_type','brand_code','brand_percentage','brand_investment','investment_amount')
           .distinct())

  IOMasterDF.createOrReplaceTempView('IOMaster_jan')
  #display(IOMasterDF)
  
  print(pathPRCIOMaster+'/'+yearIOM_1+'/'+monthIOM_1)
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
  print(pathPRCIOMaster+'/'+yearIOM+'/'+monthIOM)
  IOMasterDF=(spark.read
           .format('parquet')
           .load(pathPRCIOMaster+'/'+yearIOM+'/'+monthIOM) #
           .select('promotion_id','budget_holder','spend_type','brand_code','brand_percentage','brand_investment','investment_amount')
           .distinct())

  IOMasterDF.createOrReplaceTempView('IOMaster')
  #display(IOMasterDF)

# COMMAND ----------

BudgetHolderDF=(spark.read
              .format('parquet')
              .load(pathPRCBudgetHolder)
              .distinct())

BudgetHolderDF.createOrReplaceTempView('budget_holder')  
#display(BudgetHolderDF)

# COMMAND ----------

# LocalPHDF=(spark.read
#               .format('parquet')
#               .load(pathPRCLocalPH+'/'+yearIOM)
#               .select('PH7','Small_C')
#               .distinct())

# LocalPHDF.createOrReplaceTempView('Local_PH') 
# display(LocalPHDF)

# COMMAND ----------

spark.sql("""select * from (
select 
concat(aa.spend_type,aa.promotion_id) as IO,aa.budget_holder,aa.budget_holder_grp,aa.brand_code,bb.category,aa.brand_percentage,aa.brand_investment,
aa.investment_amount from
(select a.promotion_id,a.budget_holder,b.budget_holder_grp,a.spend_type,a.brand_code,a.brand_percentage,a.brand_investment,a.investment_amount 
from IOMaster as a left join budget_holder as b
on a.budget_holder=b.budget_holder) as aa 
left join dim_local_product as bb on 
aa.brand_code=bb.PH7_code
) a where category is not null;""").createOrReplaceTempView('parent_view')

# COMMAND ----------

(spark.sql("""select IO,category,sum(brand_investment) as total_IO_amt_PERSTR from parent_view group by IO,category""")
 .createOrReplaceTempView('IO_sum_perstr'))
 
(spark.sql("""select IO,sum(brand_investment) as total_IO_amt from parent_view group by IO""")
 .createOrReplaceTempView('IO_sum'))
 

# COMMAND ----------

# performing a left join to include the budget holder group column in the IOMaster data
IOMasterTran=spark.sql("""select 
aa.IO,aa.budget_holder,aa.budget_holder_grp,aa.brand_code,nvl(aa.category,'NA') as category,
nvl(aa.brand_percentage,0) as brand_percentage,
nvl(aa.brand_investment,0) as brand_investment,
nvl(aa.investment_amount,0) as investment_amount,
nvl(aa.total_IO_amt_PERSTR,0) as total_IO_amt_PERSTR,
nvl(bb.total_IO_amt,0) as total_IO_amt,
nvl((aa.brand_investment/aa.total_IO_amt_PERSTR),0) as proportion_PERSTR,
nvl((aa.brand_investment/bb.total_IO_amt),0) as proportion from 
(select 
a.IO,a.budget_holder,a.budget_holder_grp,a.brand_code,a.category,a.brand_percentage,a.brand_investment,a.investment_amount,b.total_IO_amt_PERSTR from parent_view as a left join IO_sum_perstr as b
on a.IO=b.IO and a.category=b.category) as  aa
left join IO_sum as bb
on aa.IO=bb.IO""")

# COMMAND ----------

# for j in range(0,11):
# monthIOM_dummy= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathPRCIOMaster +"/" +str(yearIOM))])).zfill(2)
# (IOMasterTran.repartition(1).write
#  .format('parquet')
#  .mode('overwrite')
#  .save(pathTRANIOMasterFile+'/'+yearIOM+'/'+monthIOM_dummy)
# )

# COMMAND ----------

(IOMasterTran.repartition(1).write
 .format('parquet')
 .mode('overwrite')
 .save(pathTRANIOMasterFile+'/'+yearIOM+'/'+monthIOM)
)
