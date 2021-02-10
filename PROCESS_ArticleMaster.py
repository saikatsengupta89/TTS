# Databricks notebook source
import pandas as pd
import numpy as np
from pyspark.sql import types
from pyspark.sql.functions import col

pathArticleMaster="dbfs:/mnt/adls/TTS/raw/monthly/mapping_article_master/"
pathArticleMaster_1='/dbfs/mnt/adls/TTS/raw/monthly/mapping_article_master/'
pathPRCArticleMaster='dbfs:/mnt/adls/TTS/processed/masters/mapping_article_master'

#reading the latest article master from the raw layer in adls
yearAM = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathArticleMaster)]))
monthAM= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathArticleMaster +"/" +str(yearAM))])).zfill(2)

fileName=dbutils.fs.ls(pathArticleMaster+yearAM+"/"+monthAM+"/")[0].name

# COMMAND ----------

def convertToFloat(x):
  x= str(x).replace(',','')
  if (x.find('-')> 0):
    x= float(x.replace('-','')) * -1
  else:
    x= float(x)
  return x

df= pd.read_csv(pathArticleMaster_1+"/"+yearAM+"/"+monthAM+"/"+fileName)
df= df.replace({np.nan: None})
df['SUBDIV']= df['SUBDIV'].apply(convertToFloat)
df['Ngày cập nhật']=pd.to_datetime(df['Ngày cập nhật'])
df['Ngày cập nhật'] = df['Ngày cập nhật'].apply(lambda x: x.strftime('%Y-%m-%d'))

# COMMAND ----------

articleMasterSchema= types.StructType([
  types.StructField("material_code", types.StringType()),
  types.StructField("material_desc", types.StringType()),
  types.StructField("param1", types.StringType()),
  types.StructField("price_per_su", types.FloatType()),
  types.StructField("su_type", types.StringType()),
  types.StructField("PC_per_SU", types.StringType()),
  types.StructField("cs_type", types.StringType()),
  types.StructField("PC_per_CS", types.StringType()),
  types.StructField("pc_type", types.StringType()),
  types.StructField("creation_date", types.StringType()),
  types.StructField("param2", types.StringType()),
  types.StructField("param3", types.StringType()),
  types.StructField("PH1_code", types.StringType()),
  types.StructField("PH1_description", types.StringType()),
  types.StructField("PH2_code", types.StringType()),
  types.StructField("PH2_description", types.StringType()),
  types.StructField("PH3_code", types.StringType()),
  types.StructField("PH3_description", types.StringType()),
  types.StructField("PH4_code", types.StringType()),
  types.StructField("PH4_description", types.StringType()),
  types.StructField("PH5_code", types.StringType()),
  types.StructField("PH5_description", types.StringType()),
  types.StructField("PH6_code", types.StringType()),
  types.StructField("PH6_description", types.StringType()),
  types.StructField("PH7_code", types.StringType()),
  types.StructField("PH7_description", types.StringType()),
  types.StructField("PH8_code", types.StringType()),
  types.StructField("PH8_description", types.StringType()),
  types.StructField("PH9_code", types.StringType()),
  types.StructField("PH9_description", types.StringType()),
  types.StructField("site", types.StringType()),
  types.StructField("param4", types.StringType()),
  types.StructField("param5", types.StringType()),
  types.StructField("status", types.StringType())
])

articleMasterDF= spark.createDataFrame(df, articleMasterSchema)
articleMasterDF.createOrReplaceTempView("article_master")

(
spark.sql("""
          select 
          material_code,
          material_desc,
          param1,
          price_per_su,
          su_type,
          cast(PC_per_SU as int) PC_per_SU,
          cs_type,
          cast(PC_per_CS as int) PC_per_CS,
          pc_type,
          creation_date,
          param2,
          param3,
          PH1_code,
          PH1_description,
          PH2_code,
          PH2_description,
          PH3_code,
          PH3_description,
          PH4_code,
          PH4_description,
          PH5_code,
          PH5_description,
          PH6_code,
          PH6_description,
          PH7_code,
          PH7_description,
          PH8_code,
          PH8_description,
          PH9_code,
          PH9_description,
          site,
          param4,
          param5,
          status
          from article_master
          """)
.repartition(1).write 
.format('parquet') 
.mode('overwrite') 
.save(pathPRCArticleMaster)
)

# COMMAND ----------


