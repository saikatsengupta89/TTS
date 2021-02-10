# Databricks notebook source
import pandas as pd
from pyspark.sql import types
from pyspark.sql.functions import col
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled','false')

pathLocalPH="dbfs:/mnt/adls/TTS/raw/monthly/Mapping_LocalProduct/"
pathLocalPH_1='/dbfs/mnt/adls/TTS/raw/monthly/Mapping_LocalProduct/'
pathPRCLocalPH='dbfs:/mnt/adls/TTS/processed/masters/Mapping_LocalProduct'

#reading the latest article master from the raw layer in adls
yearLocalPH = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathLocalPH)]))
monthLocalPH= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathLocalPH +"/" +str(yearLocalPH))])).zfill(2)
#dayIOM = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathIOMaster +"/"+str(yearIOM)+"/"+str(monthIOM))])).zfill(2)
fileName=dbutils.fs.ls(pathLocalPH+yearLocalPH+"/"+monthLocalPH)[0].name

# COMMAND ----------

df= pd.read_csv(pathLocalPH_1+yearLocalPH+"/"+monthLocalPH+"/"+fileName)

# COMMAND ----------

LocalPHSchema= types.StructType([
  types.StructField("material", types.StringType()),
  types.StructField("PH9", types.StringType()),
  types.StructField("PH_desc", types.StringType()),
  types.StructField("PH7", types.StringType()),
  types.StructField("CD_DP_name", types.StringType()),
  types.StructField("portfolio", types.StringType()),
  types.StructField("pack_size", types.StringType()),
  types.StructField("pack_group", types.StringType()),
  types.StructField("brand_variant", types.StringType()),
  types.StructField("sub_brand", types.StringType()),
  types.StructField("brand", types.StringType()),
  types.StructField("segment", types.StringType()),
  types.StructField("CCBT", types.StringType()),
  types.StructField("Small_C", types.StringType()),
  types.StructField("Big_C", types.StringType()),
  types.StructField("CD_View_1", types.StringType()),
  types.StructField("CD_View_2", types.StringType())
])

LocalPHDF= spark.createDataFrame(df, LocalPHSchema)

#writing the data in the parquet format in the processed layer
(LocalPHDF.repartition(1).write 
.format('parquet') 
.mode('overwrite')  
.save(pathPRCLocalPH+'/'+yearLocalPH) 
)
