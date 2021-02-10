# Databricks notebook source
import pandas as pd
import numpy as np
from pyspark.sql import types
from pyspark.sql.functions import col

pathGiftMaster="/mnt/adls/TTS/raw/monthly/gift_master/"
pathPRCGiftMaster='/mnt/adls/TTS/processed/references/ref_gift_master'

#reading the latest article master from the raw layer in adls
yearGM = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathGiftMaster)]))
monthGM= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathGiftMaster +"/" +str(yearGM))])).zfill(2)

#fileName=dbutils.fs.ls(pathGiftMaster+yearGM+"/"+monthGM+"/")[0].name

# COMMAND ----------

giftMasterSchema = types.StructType ([
  types.StructField("gift_code",types.StringType()),
  types.StructField("gift_name",types.StringType()),
  types.StructField("gift_type_name",types.StringType()),
  types.StructField("gift_type_code",types.StringType()),
  types.StructField("gift_group",types.StringType()),
  types.StructField("packing_per_case",types.IntegerType()),
  types.StructField("UOM",types.StringType()),
  types.StructField("remaining_sl_policy",types.IntegerType()),
  types.StructField("color",types.StringType()),
  types.StructField("size_h",types.DoubleType()),
  types.StructField("size_w",types.DoubleType()),
  types.StructField("size_l",types.DoubleType()),
  types.StructField("grossweight_per_case_kg",types.DoubleType()),
  types.StructField("netweight_per_pc_gm",types.DoubleType()),
  types.StructField("dimension_per_case_h",types.DoubleType()),
  types.StructField("dimension_per_case_w",types.DoubleType()),
  types.StructField("description",types.StringType()),
  types.StructField("enabled",types.StringType()),
  types.StructField("vat",types.StringType())
])

giftMasterDF= (spark.read
               .option('header',True)
               .schema(giftMasterSchema)
               .csv(pathGiftMaster+yearGM+"/"+monthGM+"/"))
#createDataFrame(df, giftMasterSchema)
#giftMasterDF.createOrReplaceTempView("gift_master")

# for j in range(1,12):
#   monthGM_dummy= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathGiftMaster +"/" +str(yearGM))])-j).zfill(2)
#   (giftMasterDF
#    .write.mode("overwrite")
#    .parquet(pathPRCGiftMaster+"/"+yearGM+"/"+monthGM_dummy+"/"))


(giftMasterDF
 .write.mode("overwrite")
 .parquet(pathPRCGiftMaster+"/"+yearGM+"/"+monthGM+"/"))

# COMMAND ----------


