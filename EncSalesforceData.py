# Databricks notebook source
"""
Author       : Saikat Sengupta
Created Date : 05/12/2020
Modified Date: 06/12/2020
Purpose      : Test Encryption and Decryption on PII fields for Salesforce data
"""

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# MAGIC %run ./registerEncryption

# COMMAND ----------

masterKeyFilePath="/dbfs/mnt/adls/EDGE_Analytics/TestEncryption/masterKeyFile.asc"

with open(masterKeyFilePath, 'r') as f:
  lines= f.readlines()

MASTER_KEY= str(lines[0])

# COMMAND ----------

salesforcePath="/mnt/adls/EDGE_Analytics/Datalake/Staging/Incremental/Masters/Salesforce_Hierarchy"
csvPath    ="/mnt/adls/EDGE_Analytics/TestEncryption/csv"
parquetPath="/mnt/adls/EDGE_Analytics/TestEncryption/parquet"

yearSF = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (salesforcePath)]))
monthSF= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (salesforcePath +"/" +str(yearSF))])).zfill(2)
daySF  = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (salesforcePath +"/"+str(yearSF)+"/"+str(monthSF))])).zfill(2)

dataSF= spark.read.option("header","true").csv(salesforcePath+"/"+yearSF+"/"+monthSF+"/"+daySF)
dataSF.createOrReplaceTempView ("master_salesforce")

# COMMAND ----------

# below sample code showing how to use dataframe UDF
encryptDF = dataSF.select(col("Sold-to party").alias("sold_to_party"),
                          col("Ship-to party").alias("sold_to_party"),
                          col("Role").alias("role"),
                          col ("Role ID").alias("role_id"),
                          col("Role Name").alias("role_name"),
                          col("Role Business Scope Assignment").alias("role_business_scope_assignment")
                         )\
                  .withColumn("role_name", encrypt_pii("role_name", lit(MASTER_KEY)))
display(encryptDF)

# COMMAND ----------

# below sample code showing how to use spark SQL UDF
encryptDF = spark.sql ("""
            select 
            `Sold-to party` as sold_to_party,
            `Ship-to party` as ship_to_party,
            Role as role,
            `Role ID` as role_id,
            encrypt_pii (`Role Name`, '{}') role_name,
            `Role Business Scope Assignment` as role_business_scope_assignment,
            encrypt_pii (`Role Email`, '{}') email,
            `Role Phone` as role_phone,
            `Role Title` as role_title,
            `LSR 2 Area` as lsr2_area,
            `Primary Sales Organisation` as primary_sales_organisation,
            Banner as banner,
            `TFML 2` as tmfl2,
            `Created by` as created_by,
            `Created Time Stamp` as created_ts,
            `Changed by` as changed_by,
            `Changed Time Stamp` as changed_ts
            from master_salesforce
            """.format(MASTER_KEY, MASTER_KEY)
          )
encryptDF.createOrReplaceTempView("salesman_encrypt")
display(encryptDF)

# COMMAND ----------

# writing as a CSV
encryptDF.repartition(1) \
         .write \
         .mode("overwrite") \
         .csv(csvPath)

# writing as a PARQUET
encryptDF.repartition(1) \
         .write \
         .mode("overwrite") \
         .parquet(parquetPath)

# COMMAND ----------

readCsvDF= spark.read.option("inferSchema","True").csv(csvPath)
display(readCsvDF)

# COMMAND ----------

readParqDF= spark.read.parquet(parquetPath)
display(readParqDF)
readParqDF.createOrReplaceTempView("salesforce_encrypted")

# COMMAND ----------

decryptParqDF= spark.sql("""
                         select 
                         sold_to_party,
                         ship_to_party,
                         role,
                         role_id,
                         decrypt_pii (role_name, '{}') role_name,
                         role_business_scope_assignment,
                         decrypt_pii (email, '{}') email,
                         role_phone,
                         role_title,
                         lsr2_area,
                         primary_sales_organisation,
                         banner,
                         tmfl2,
                         created_by,
                         created_ts,
                         changed_by,
                         changed_ts
                         from salesforce_encrypted
                        """.format(MASTER_KEY, MASTER_KEY)
                        )
display(decryptParqDF)
