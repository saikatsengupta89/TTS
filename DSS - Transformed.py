# Databricks notebook source
from pyspark.sql import types

salesmanMaster= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Salesman/"
spgToPHMap    = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/SPG_TO_PH_MAP/"
prodPath      = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Product/"
histOutletMap = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Hist_Outlet_Map"
outletSPG     = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Hist_SPG_Map"

# COMMAND ----------

yearSM = str(max([i.name.replace('/','') for i in dbutils.fs.ls (salesmanMaster)]))
monthSM= str(max([i.name.replace('/','') for i in dbutils.fs.ls (salesmanMaster +"/" +str(yearSM))])).zfill(0)
daySM  = str(max([i.name.replace('/','') for i in dbutils.fs.ls (salesmanMaster +"/"+str(yearSM)+"/"+str(monthSM))])).zfill(0)

yearPH = str(max([i.name.replace('/','') for i in dbutils.fs.ls (spgToPHMap)]))
monthPH= str(max([i.name.replace('/','') for i in dbutils.fs.ls (spgToPHMap +"/" +str(yearPH))])).zfill(0)
dayPH  = str(max([i.name.replace('/','') for i in dbutils.fs.ls (spgToPHMap +"/"+str(yearPH)+"/"+str(monthPH))])).zfill(0)

yearPD = str(max([i.name.replace('/','') for i in dbutils.fs.ls (prodPath)]))
monthPD= str(max([i.name.replace('/','') for i in dbutils.fs.ls (prodPath +"/" +str(yearPD))])).zfill(0)
dayPD  = str(max([i.name.replace('/','') for i in dbutils.fs.ls (prodPath +"/"+str(yearPD)+"/"+str(monthPD))])).zfill(0)

om_schema = types.StructType([
  types.StructField("date",types.StringType()),
  types.StructField("old_outlet_code",types.StringType()),
  types.StructField("master_outlet_code",types.StringType()),
  types.StructField("master_distributor_code",types.StringType()),
  types.StructField("master_site_code",types.StringType())
])

dataOM= spark.read.option("header","true").schema(om_schema).csv(histOutletMap)
dataOM.createOrReplaceTempView ("hist_outlet_map")

dataSM= spark.read.option("header","true").csv(salesmanMaster+yearSM+"/"+monthSM+"/"+daySM)
dataSM.createOrReplaceTempView ("dim_salesman")

dataPH= spark.read.option("header","true").csv(spgToPHMap+yearPH+"/"+monthPH+"/"+dayPH)
dataPH.createOrReplaceTempView ("spg_to_ph_map")

dataPD= spark.read.option("header","true").csv(prodPath+yearPD+"/"+monthPD+"/"+dayPD)
dataPD.createOrReplaceTempView ("dim_product")

outletSPGDF = spark.read.parquet(outletSPG)
outletSPGDF.createOrReplaceTempView("dim_master_outlet_spg")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists view_fact_daily_sales;
# MAGIC create temporary view view_fact_daily_sales as
# MAGIC select 
# MAGIC country_code,
# MAGIC product_code,
# MAGIC dss.division,
# MAGIC invoice_number,
# MAGIC site_code_year,
# MAGIC transactional_distributor_code,
# MAGIC master_distributor_code,
# MAGIC transactional_site_code,
# MAGIC master_site_code,
# MAGIC transactional_outlet_code,
# MAGIC dss.master_outlet_code,
# MAGIC invoice_date,
# MAGIC transactional_salesman_code,
# MAGIC master_salesman_code,
# MAGIC salesman_product_group_code,
# MAGIC transactional_spg,
# MAGIC master_spg,
# MAGIC dss.no_of_salespersons,
# MAGIC invoicetype,
# MAGIC customer_ZRSS_flag,
# MAGIC sales_ret_ref_invoice_number,
# MAGIC gross_sales_val,
# MAGIC gross_sales_val_mn,
# MAGIC net_invoice_val,
# MAGIC gross_sales_return_val,
# MAGIC sales_return_val,
# MAGIC on_invoice_discount_val,
# MAGIC value_added_tax,
# MAGIC off_inv_discount_val,
# MAGIC turn_over_val,
# MAGIC sales_with_return_pc_qty,
# MAGIC sales_return_pc_qty,
# MAGIC sales_pc_qty,
# MAGIC sales_cs_vol,
# MAGIC sales_kg_vol,
# MAGIC sales_lt_vol,
# MAGIC free_pc_qty,
# MAGIC salesforce,
# MAGIC brand,
# MAGIC category_code,
# MAGIC brand_cat_core,
# MAGIC year_id,
# MAGIC month_id,
# MAGIC dss.time_key
# MAGIC from
# MAGIC (select 
# MAGIC   country_code,
# MAGIC   dss.product_code,
# MAGIC   pd.division,
# MAGIC   invoice_number,
# MAGIC   site_code_year,
# MAGIC   transactional_distributor_code,
# MAGIC   nvl(master_distributor_code, transactional_distributor_code) as master_distributor_code,
# MAGIC   transactional_site_code,
# MAGIC   nvl(master_site_code, transactional_site_code) as master_site_code,
# MAGIC   transactional_outlet_code,
# MAGIC   nvl(lpad(om.master_outlet_code,10,'0'), transactional_outlet_code) as master_outlet_code,
# MAGIC   invoice_date,
# MAGIC   transactional_salesman_code,
# MAGIC   sm.salesman_product_group_code,
# MAGIC   sm.standardised_salesman_product_group transactional_spg, 
# MAGIC   nvl(cast(sm.no_of_salespersons_serving_outlet_in_this_spg as int),0) no_of_salespersons,
# MAGIC   invoicetype,
# MAGIC   customer_ZRSS_flag,
# MAGIC   sales_ret_ref_invoice_number,
# MAGIC   gross_sales_val,
# MAGIC   gross_sales_val_mn,
# MAGIC   net_invoice_val,
# MAGIC   gross_sales_return_val,
# MAGIC   sales_return_val,
# MAGIC   on_invoice_discount_val,
# MAGIC   value_added_tax,
# MAGIC   off_inv_discount_val,
# MAGIC   turn_over_val,
# MAGIC   sales_with_return_pc_qty,
# MAGIC   sales_return_pc_qty,
# MAGIC   sales_pc_qty,
# MAGIC   sales_cs_vol,
# MAGIC   sales_kg_vol,
# MAGIC   sales_lt_vol,
# MAGIC   free_pc_qty,
# MAGIC   dss.salesforce,
# MAGIC   dss.brand,
# MAGIC   dss.category_code,
# MAGIC   brand_cat_core,
# MAGIC   year_id,
# MAGIC   month_id,
# MAGIC   time_key
# MAGIC   from fact_daily_sales dss
# MAGIC   left outer join hist_outlet_map om 
# MAGIC   on dss.transactional_outlet_code = lpad(om.old_outlet_code,10,'0')
# MAGIC   left outer join dim_salesman sm 
# MAGIC   on dss.transactional_salesman_code= sm.salesman_code
# MAGIC   left outer join dim_product pd
# MAGIC   on dss.product_code = pd.product_code
# MAGIC  ) dss
# MAGIC  left outer join dim_master_outlet_spg spg
# MAGIC  on dss.master_outlet_code= spg.master_outlet_code
# MAGIC  and dss.division= spg.division;

# COMMAND ----------

# %sql
# select 
# time_key,
# transactional_outlet_code,
# master_outlet_code,
# transactional_distributor_code,
# master_distributor_code,
# product_code,
# division,
# transactional_salesman_code,
# master_salesman_code,
# salesman_product_group_code,
# transactional_spg,
# master_spg
# from view_fact_daily_sales where master_outlet_code='0003078095'
# order by 1 desc;

# COMMAND ----------

# %sql
# select 
# time_key,
# transactional_outlet_code,
# master_outlet_code,
# product_code,
# division,
# transactional_salesman_code,
# master_salesman_code,
# salesman_product_group_code,
# transactional_spg,
# master_spg
# from view_fact_daily_sales where master_outlet_code='0003078095'
# order by 1 desc;
