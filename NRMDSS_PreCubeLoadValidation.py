# Databricks notebook source
processingYear  =dbutils.widgets.get("ProcessingYear")
processingMonth =dbutils.widgets.get("ProcessingMonth")
processingMonthID= processingYear + processingMonth

# COMMAND ----------

exceptionPromoValAgg = spark.sql("""
                              select 
                                nvl(trn.promotion_type, agg.promotion_type) promotion_type,
                                nvl(trn.promotion_mechanism, agg.promotion_mechanism) promotion_mechanism,
                                nvl(trn.promotion_value,0) trn_promotion_value,
                                nvl(agg.promotion_value,0) agg_promotion_value,
                                (abs(nvl(agg.promotion_value,0)) - abs(nvl(trn.promotion_value,0))) delta_promotion_value
                              from
                              (select 
                                  promotion_type,
                                  promotion_mechanism,
                                  round(sum(promotion_value)) promotion_value
                                from edge.fact_monthly_promo_agg 
                                where year_id={} and month_id={}
                                and promotion_mechanism not in ('JR','NU')
                                group by promotion_type, promotion_mechanism
                              ) agg
                              full outer join
                              (select 
                                  promotion_type,
                                  promotion_mechanism,
                                  round(sum(promotion_value)) promotion_value
                                from edge.fact_daily_promotions 
                                where year_id={} and month_id={}
                                and promotion_mechanism not in ('JR','NU')
                                group by promotion_type, promotion_mechanism
                              ) trn
                              on trn.promotion_mechanism= agg.promotion_mechanism 
                              and trn.promotion_type= agg.promotion_type
                              """.format(processingYear, processingMonthID, processingYear, processingMonthID)
                           )
exceptionPromoValAgg.createOrReplaceTempView("exception_promo_values_agg")

# COMMAND ----------

# MATCH NRM DAILY AGGREGATE PROMOTION VALUES AGAINST NRM MONTHLY AGGREGATE
exceptionAggValidation= spark.sql("""
                                   select * 
                                   from exception_promo_values_agg
                                   where abs(delta_promotion_value) > 0
                                  """)

if(exceptionAggValidation.count() > 0):
  raise Exception ("Exception: NRM daily not matching with NRM monthly aggregate")
else:
  print("NRM daily matching with NRM monthly aggregate. Proceed CUBE refresh")
