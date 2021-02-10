# Databricks notebook source
processingYear  =dbutils.widgets.get("ProcessingYear")
processingMonth =dbutils.widgets.get("ProcessingMonth")
processingMonthID= processingYear + processingMonth
validationThreshold = float(dbutils.widgets.get("ValidationThreshold"))

# COMMAND ----------

exceptionPromoVal = spark.sql("""
                              select 
                                nvl(trn.promotion_type, prc.promotion_type) promotion_type,
                                nvl(trn.promotion_mechanism, prc.promotion_mechanism) promotion_mechanism,
                                nvl(trn.promotion_value,0) trn_promotion_value,
                                nvl(prc.promotion_value,0) prc_promotion_value,
                                case when abs(nvl(prc.promotion_value,0))=0 then 0
                                     else ((abs(nvl(prc.promotion_value,0)) - abs(nvl(trn.promotion_value,0)))/abs(nvl(prc.promotion_value,0)) * 100) 
                                     end delta_prcntg
                              from
                              (select 
                                  promotion_type,
                                  promotion_mechanism,
                                  round(sum(promotion_value)) promotion_value
                                from edge.fact_daily_promotions 
                                where year_id={} and month_id={}
                                and promotion_mechanism not in ('JR','NU')
                                group by promotion_type, promotion_mechanism
                              ) trn
                              full outer join
                              (select 
                                  promotion_type,
                                  promotion_mechanism,
                                  round(sum(promotion_value)) promotion_value
                                from edge.processed_nrm_data 
                                where year_id={} and month_id={}
                                and promotion_mechanism not in ('JR','NU')
                                group by promotion_type, promotion_mechanism
                              ) prc
                              on trn.promotion_mechanism= prc.promotion_mechanism 
                              and trn.promotion_type= prc.promotion_type
                              """.format(processingYear, processingMonthID, processingYear, processingMonthID)
                           )
exceptionPromoVal.createOrReplaceTempView("exception_promo_values")

# COMMAND ----------

# PROMOTION TYPE - BBFREE AND PROMOTION MECHANISM - FQ IS NOT CONSIDERED AS ITS DERIVED IN THE PROCESS
breachedThresholdPromo= spark.sql("""
                                   select * 
                                   from exception_promo_values
                                   where ( delta_prcntg >= {}
                                           and promotion_type !='BBFREE' and promotion_mechanism != 'FQ'
                                         )
                                  """.format(validationThreshold)
                                 )

if(breachedThresholdPromo.count() > 0):
  raise Exception ("Exception: Summary Vaidation failed")
else:
  print("Promotion Values pre and post processing within threshold. No outliers.")
