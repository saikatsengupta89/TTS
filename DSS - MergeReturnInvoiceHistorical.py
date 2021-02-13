# Databricks notebook source
processingYear =  int(dbutils.widgets.get("ProcessingYear"))
processingMonth = int(dbutils.widgets.get("ProcessingMonth"))

print(processingYear)
print(processingMonth)

# COMMAND ----------

from pyspark.sql.functions import col, date_format, unix_timestamp, from_unixtime, date_sub

fetchDateParamDF= spark.sql("""
                            select 
                            processing_year curr_year,
                            case when processing_month=1 then (processing_year -1)
                                 else processing_year
                                 end prev_year,
                            case when processing_month=1 then 12
                                 else (processing_month -1)
                                 end prev_month_id,
                            processing_month month_id
                            from 
                            (select {} as processing_year, {} as processing_month) q
                            """.format(processingYear, processingMonth))
dateParamList= fetchDateParamDF.rdd.flatMap(lambda x: x).collect()
currYear = int(dateParamList[0])
prevYear = int(dateParamList[1])
currMonth= int(dateParamList[3])
prevMonth= int(dateParamList[2])
currMonthID = str(currYear)+ str(currMonth).zfill(2)
prevMonthID = str(prevYear)+ str(prevMonth).zfill(2)

print(currYear, prevYear, currMonthID, prevMonthID)

# COMMAND ----------

spark.sql ("""
merge into fact_daily_sales tgt 
using 
(select * 
  from 
  (select 
    q1.invoice_number,
    q1.sales_ret_ref_invoice_number,
    q1.invoice_date,
    q1.product_code,
    q2.invoice_date as updated_invoice_date,
    q2.time_key as updated_time_key,
    q2.month_id as updated_month_id,
    q2.year_id as updated_year_id
    from
    (select 
        distinct
        invoice_number, 
        sales_ret_ref_invoice_number,
        product_code, 
        invoice_date
      from fact_daily_sales 
      where year_id={} 
      and month_id ={}
      and sales_ret_ref_invoice_number != '0'
    ) q1
    left outer join
    (select 
        distinct
        invoice_number, 
        product_code, 
        invoice_date,
        time_key,
        month_id,
        year_id
      from fact_daily_sales 
      where year_id in ({},{})
      and month_id in ({},{})
    ) q2 
    on q1.sales_ret_ref_invoice_number= q2.invoice_number 
    and q1.product_code= q2.product_code
  ) q where updated_invoice_date is not null
) src
on 
( tgt.year_id in ({},{})
  and month_id in ({},{})
  and tgt.invoice_number = src.invoice_number
  and tgt.product_code= src.product_code
)
when matched then update set tgt.invoice_date= src.updated_invoice_date,
                             tgt.time_key    = src.updated_time_key,
                             tgt.month_id    = src.updated_month_id,
                             tgt.year_id     = src.updated_year_id,
                             tgt.update_ts   = current_timestamp()
""".format(currYear, 
           currMonthID, 
           prevYear, 
           currYear, 
           prevMonthID, 
           currMonthID, 
           prevYear, 
           currYear, 
           prevMonthID, 
           currMonthID)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC analyze table fact_daily_sales compute statistics;
# MAGIC optimize fact_daily_sales zorder by (transactional_outlet_code, transactional_distributor_code);
