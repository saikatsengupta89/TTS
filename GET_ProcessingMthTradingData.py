# Databricks notebook source
tradingDayPath= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Masters/Time/"

processingYear  =dbutils.widgets.get("ProcessingYear")
processingMonth =dbutils.widgets.get("ProcessingMonth")
yearMonth= int(processingYear+processingMonth)

# COMMAND ----------

#get the latest trading day snapshot and create the required trading Day dataframe
yearTD = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (tradingDayPath)]))
monthTD= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (tradingDayPath +"/" +str(yearTD))])).zfill(2)
dayTD  = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (tradingDayPath +"/"+str(yearTD)+"/"+str(monthTD))])).zfill(2)
# print(maxYearTD)
# print(maxMonthTD)
# print(maxDayTD)
tradingDayDF= spark.read.option("header","true") \
                        .option("inferSchema","true") \
                        .csv("/mnt/adls/EDGE_Analytics/Datalake/Transformed/Masters/Time/"+yearTD+"/"+monthTD+"/"+dayTD)
tradingDayDF.createOrReplaceTempView("trading_day_transformed")
tradingDayDF_T1= spark.sql("select "+
                           "Dt as time_key, "+
                           "year_month, "+
                           "Trading_day as trading_day "+
                           "from trading_day_transformed "+
                          f"where year_month={yearMonth} "+
                           "order by 1")
tradingDayDF_T1.createOrReplaceTempView("dim_time")
