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

distributorDF.createOrReplaceTempView("tran_distributor")

distributorDF_T1= spark.sql("select distinct "+
                            "distributor_code "+
                            "from tran_distributor "+
                            "where country_code ='VN' "+
                            "and distributor_type = 'C10000' "+
                            "and distributor_code is not null"
                            )
distributorDF_T1.createOrReplaceTempView("distributor_dt")

siteDF= spark.sql("select distinct "+
                  "site_code "+
                  "from tran_distributor "+
                  "where country_code ='VN' "+
                  "and distributor_type = 'C10000'"
                 )
siteDF.createOrReplaceTempView ("site_dt")

# COMMAND ----------

distSiteList= spark.read.option("header","true") \
                        .option("inferSchema","true") \
                        .csv(distSiteFilePath)
distSiteList.createOrReplaceTempView("distributor_lkp")
#display(distSiteList)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* validate distributor list with full scope */
# MAGIC select * from (
# MAGIC select distinct s.site_code, lkp.Site
# MAGIC from site_dt s
# MAGIC full outer join (select * from distributor_lkp where `Distributor Type`='DT DISTRIBUTOR') lkp
# MAGIC on s.site_code= lkp.Site
# MAGIC ) Q
# MAGIC where (site is null or Site is null)
# MAGIC order by 1,2
