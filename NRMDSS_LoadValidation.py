# Databricks notebook source
# MAGIC %sql
# MAGIC /* Check if for any trading day there are no transaction. 
# MAGIC    If so those are exceptions */
# MAGIC truncate table edge.check_trade_day_with_no_txn;
# MAGIC insert into edge.check_trade_day_with_no_txn
# MAGIC select * 
# MAGIC from 
# MAGIC (with check_no_trade as (
# MAGIC   select 
# MAGIC     cast(nrm.time_key as int) time_key,
# MAGIC     cast(t.trading_day as int) trading_day_flag,
# MAGIC     count(1) tot_count
# MAGIC     from nrm_raw_data nrm
# MAGIC     inner join dim_time t 
# MAGIC     on nrm.time_key= t.time_key
# MAGIC     group by nrm.time_key, cast(t.trading_day as int)
# MAGIC   )
# MAGIC   select * 
# MAGIC   from check_no_trade 
# MAGIC   where trading_day_flag=1 and tot_count = 0
# MAGIC )q;

# COMMAND ----------

# raise exception and stop the load if there are trading days with no transaction counts
check1_df= spark.sql("select * from edge.check_trade_day_with_no_txn")

# CLEAN ALL TEMPORARY PANDAS IN-MEM DATAFRAME BEFORE RAISING EXCEPTION
if (check1_df.count() > 0):
  if (int(len(nrmRawDTRDIS.index)) > 0):
    del nrmRawDTRDIS
  if (int(len(nrmRawLINDIS.index)) > 0):
    del nrmRawLINDIS
  if (int(len(nrmRawBASKET.index)) > 0):
    del nrmRawBASKET
  if (int(len(nrmRawVOLDIS.index)) > 0):
    del nrmRawVOLDIS
  if (int(len(nrmRawOTH.index)) > 0):
    del nrmRawOTH
  raise Exception ("Exception: There are cases where on a trading day there has been no transactions")
# except Exception as e:
#   raise dbutils.notebook.exit(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* check if any site maps to more than one distributors. If so raise exceptions */
# MAGIC truncate table edge.site_with_more_than_one_dt;
# MAGIC insert into edge.site_with_more_than_one_dt
# MAGIC select * 
# MAGIC from 
# MAGIC (with dup_site_list as 
# MAGIC   (select 
# MAGIC     site_code
# MAGIC     from 
# MAGIC     (select 
# MAGIC       distributor_code, site_code
# MAGIC       from nrm_raw_data
# MAGIC       group by 
# MAGIC       distributor_code, 
# MAGIC       site_code
# MAGIC     )
# MAGIC     group by site_code having count(1) > 1
# MAGIC   )
# MAGIC   select 
# MAGIC   cast(time_key as int) time_key, 
# MAGIC   distributor_code, 
# MAGIC   txn.site_code, 
# MAGIC   invoice_no
# MAGIC   from nrm_raw_df_t1 txn
# MAGIC   inner join dup_site_list lst on txn.site_code= lst.site_code
# MAGIC )q;

# COMMAND ----------

# raise exception for any distributor code and site code duplication
check2_df= spark.sql("select * from edge.site_with_more_than_one_dt")

# CLEAN ALL TEMPORARY PANDAS IN-MEM DATAFRAME BEFORE RAISING EXCEPTION
if (check2_df.count() > 0):
  if (int(len(nrmRawDTRDIS.index)) > 0):
    del nrmRawDTRDIS
  if (int(len(nrmRawLINDIS.index)) > 0):
    del nrmRawLINDIS
  if (int(len(nrmRawBASKET.index)) > 0):
    del nrmRawBASKET
  if (int(len(nrmRawVOLDIS.index)) > 0):
    del nrmRawVOLDIS
  if (int(len(nrmRawOTH.index)) > 0):
    del nrmRawOTH
  raise Exception ("Exception: There are cases where one site code has multiple distributors")
# except Exception as e:
#   raise dbutils.notebook.exit(e)
