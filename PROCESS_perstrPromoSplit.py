# Databricks notebook source
import pandas as pd
import numpy as np
from pyspark.sql import types
from pyspark.sql.functions import col

pathperstrPromoSplit="dbfs:/mnt/adls/TTS/raw/static/perstr_promotion_split"
pathPRCperstrPromoSplit='dbfs:/mnt/adls/TTS/processed/references/ref_perstr_promotion_split'

#reading the latest article master from the raw layer in adls
yearPPS = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathperstrPromoSplit)]))
monthPPS= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathperstrPromoSplit +"/" +str(yearPPS))])).zfill(2)
dayPPS= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathperstrPromoSplit +"/" +str(yearPPS)+"/"+str(monthPPS))])).zfill(2)

#fileName=dbutils.fs.ls(pathGiftMaster+yearGM+"/"+monthGM+"/")[0].name

# COMMAND ----------

split_schema = types.StructType([
  types.StructField("year",types.StringType()),
  types.StructField("quarter",types.StringType()),
  types.StructField("channel",types.StringType()),
  types.StructField("Fabsol",types.DoubleType()),
  types.StructField("Fabsen",types.DoubleType()),
  types.StructField("HNH",types.DoubleType()),
  types.StructField("Hair",types.DoubleType()),
  types.StructField("Skincare",types.DoubleType()),
  types.StructField("Skincleansing",types.DoubleType()),
  types.StructField("Oral",types.DoubleType()),
  types.StructField("Savoury",types.DoubleType()),
  types.StructField("Tea",types.DoubleType())
])

promoSplitDF= (spark.read
               .option('header',True)
               .schema(split_schema)
               .csv(pathperstrPromoSplit+"/"+yearPPS+"/"+monthPPS+"/"+dayPPS+"/"))
#createDataFrame(df, giftMasterSchema)
#giftMasterDF.createOrReplaceTempView("gift_master")

(promoSplitDF
 .write.mode("overwrite")
 .parquet(pathPRCperstrPromoSplit+"/"+yearPPS+"/"))

# COMMAND ----------


