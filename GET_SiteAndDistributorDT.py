# Databricks notebook source
distributorFilePath= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Distributor/"
distSiteFilePath   = "/mnt/adls/TTS/raw/static/mapping_dist_site_list"

# COMMAND ----------

yearTD = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (distributorFilePath)]))
monthTD= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (distributorFilePath +"/" +str(yearTD))])).zfill(2)
dayTD  = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (distributorFilePath +"/"+str(yearTD)+"/"+str(monthTD))])).zfill(2)

distributorDF= spark.read.option("header","true") \
                         .option("inferSchema","true") \
                         .csv(distributorFilePath+yearTD+"/"+monthTD+"/"+dayTD)

# distributorDF= spark.read.option("header","true") \
#                          .option("inferSchema","true") \
#                          .csv(distributorFilePath)

distributorDF.createOrReplaceTempView("dim_distributor")

distributorDF_T1= spark.sql("select distinct "+
                            "distributor_code "+
                            "from dim_distributor "+
                            "where country_code ='VN' "+
                            "and distributor_type = 'C10000' "+
                            "and distributor_code is not null"
                            )
distributorDF_T1.createOrReplaceTempView("list_distributor")

siteDF= spark.sql("select distinct "+
                  "site_code "+
                  "from dim_distributor "+
                  "where country_code ='VN' "+
                  "and distributor_type = 'C10000'"
                 )
siteDF.createOrReplaceTempView ("list_site")

# COMMAND ----------

distSiteList= spark.read.option("header","true") \
                        .option("inferSchema","true") \
                        .csv(distSiteFilePath)
distSiteList.createOrReplaceTempView("distributor_lkp")
#display(distSiteList)

# COMMAND ----------

# %sql
# /* validate distributor list with full scope */
# select * from (
# select distinct s.site_code, lkp.Site
# from list_site s
# full outer join (select * from distributor_lkp where `Distributor Type`='DT DISTRIBUTOR') lkp
# on s.site_code= lkp.Site
# ) Q
# where (site is null or Site is null)
# order by 1,2
