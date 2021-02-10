# Databricks notebook source
from pyspark.sql import types
from pyspark.sql.functions import col
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled','false')

pathBudgetHolder="dbfs:/mnt/adls/TTS/raw/static/mapping_budget_holder/"
pathPRCBudgetHolder='dbfs:/mnt/adls/TTS/processed/masters/mapping_budget_holder'

#reading the latest article master from the raw layer in adls
yearBH = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathBudgetHolder)]))
monthBH= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathBudgetHolder +"/" +str(yearBH))])).zfill(2)
dayBH = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(pathBudgetHolder +"/"+str(yearBH)+"/"+str(monthBH))])).zfill(2)
#fileName=dbutils.fs.ls(pathIOMaster+yearBH+"/"+monthBH)[0].name

# COMMAND ----------

BudgetHolderSchema= types.StructType([
  types.StructField("budget_holder", types.StringType()),
  types.StructField("budget_holder_grp", types.StringType())
])

BudgetHolderDF=(spark.read
                .option('header',True)
                .schema(BudgetHolderSchema)
                .csv(pathBudgetHolder+yearBH+"/"+monthBH+'/'+dayBH)
               )

# COMMAND ----------

(BudgetHolderDF.repartition(1).write
 .mode('overwrite')
 .parquet(pathPRCBudgetHolder)
)
