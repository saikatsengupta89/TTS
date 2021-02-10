# Databricks notebook source
from pyspark.sql import types
from pyspark.sql.functions import col
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled','false')

pathPromoAll="dbfs:/mnt/adls/TTS/raw/static/mapping_budget_holder/"
pathPRCPromoAll='dbfs:/mnt/adls/TTS/processed/masters/mapping_budget_holder'

#reading the latest article master from the raw layer in adls
yearBH = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathBudgetHolder)]))
monthBH= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathBudgetHolder +"/" +str(yearBH))])).zfill(2)
dayBH = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathBudgetHolder +"/"+str(yearBH)+"/"+str(monthBH))])).zfill(2)
#fileName=dbutils.fs.ls(pathIOMaster+yearBH+"/"+monthBH)[0].name

# COMMAND ----------

path_promo_detailed='/mnt/adls/TTS/processed/masters/mapping_channel'
df=(spark.read
    .parquet(path_promo_detailed))
