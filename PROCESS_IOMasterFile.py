# Databricks notebook source
import pandas as pd
from pyspark.sql import types
from pyspark.sql.functions import col
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled','false')

pathIOMaster="dbfs:/mnt/adls/TTS/raw/monthly/mapping_io_master/"
pathIOMaster_1='/dbfs/mnt/adls/TTS/raw/monthly/mapping_io_master/'
pathPRCIOMaster='dbfs:/mnt/adls/TTS/processed/references/ref_mthly_io'

#reading the latest article master from the raw layer in adls
yearIOM = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathIOMaster)]))
monthIOM= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathIOMaster +"/" +str(yearIOM))])).zfill(2)
#dayIOM = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathIOMaster +"/"+str(yearIOM)+"/"+str(monthIOM))])).zfill(2)
fileName=dbutils.fs.ls(pathIOMaster+yearIOM+"/"+monthIOM)[0].name

# COMMAND ----------

df= pd.read_csv(pathIOMaster_1+yearIOM+"/"+monthIOM+"/"+fileName)

# COMMAND ----------

IOMasterSchema= types.StructType([
  types.StructField("promotion_id", types.StringType()),
  types.StructField("promotion_status", types.StringType()),
  types.StructField("fund_type", types.StringType()),
  types.StructField("promotion_name", types.StringType()),
  types.StructField("created_date", types.StringType()),
  types.StructField("start_date", types.StringType()),
  types.StructField("end_date", types.StringType()),
  types.StructField("sellout_start_date", types.StringType()),
  types.StructField("sellout_end_date", types.StringType()),
  types.StructField("creator_id", types.StringType()),
  types.StructField("budget_holder", types.StringType()),
  types.StructField("spend_type", types.StringType()),
  types.StructField("investment_type", types.StringType()),
  types.StructField("brand_code", types.StringType()),
  types.StructField("brand_percentage", types.DoubleType()),
  types.StructField("brand_investment", types.DoubleType()),
  types.StructField("investment_amount", types.DoubleType()),
  types.StructField("customer_code", types.StringType()),
  types.StructField("account_allocation_percentage", types.DoubleType()),
  types.StructField("account_investment_per_brand", types.DoubleType()),
  types.StructField("sales_organisation_id", types.StringType()),
])

IOMasterDF= spark.createDataFrame(df, IOMasterSchema)

#writing the data in the parquet format in the processed layer
(IOMasterDF.repartition(3).write 
.format('parquet') 
.mode('overwrite')  
.save(pathPRCIOMaster+'/'+yearIOM+'/'+monthIOM) 
)


# COMMAND ----------


