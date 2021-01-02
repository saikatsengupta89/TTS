# Databricks notebook source
deltaPathDailyPromotions="/mnt/adls/TTS/transformed/facts/fact_daily_promotions"

# COMMAND ----------

# MAGIC %md ##### call Final Aggregate Notebook

# COMMAND ----------

# MAGIC %run ./NRMDSS_DailyAggregate

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists edge.fact_daily_promotions;
# MAGIC create table if not exists edge.fact_daily_promotions
# MAGIC (
# MAGIC invoice_number string,
# MAGIC product_code string,
# MAGIC standard_product_group_code string,
# MAGIC promotion_id string,
# MAGIC IO string,
# MAGIC promotion_mechanism string,
# MAGIC promotion_desc string,
# MAGIC promotion_type string,
# MAGIC budget_holder_group string,
# MAGIC invoice_type string,
# MAGIC promo_start_date date,
# MAGIC promo_end_date date,
# MAGIC value_based_promo_disc double,
# MAGIC header_lvl_disc double,
# MAGIC free_quantity_value double,
# MAGIC promotion_value double,
# MAGIC distributor_code string,
# MAGIC site_code string,
# MAGIC outlet_code string,
# MAGIC banded string,
# MAGIC gift_price_available string,
# MAGIC country string,
# MAGIC year_id int,
# MAGIC month_id int,
# MAGIC time_key int
# MAGIC )
# MAGIC USING DELTA 
# MAGIC PARTITIONED BY (year_id, month_id, time_key)
# MAGIC LOCATION "/mnt/adls/TTS/transformed/facts/fact_daily_promotions";
# MAGIC 
# MAGIC ALTER TABLE edge.fact_daily_promotions
# MAGIC SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

# COMMAND ----------

transformed_nrm_data.createOrReplaceTempView("final_nrm_data")
nrmDeltaDF= spark.sql("""select 
                          cast(invoice_no as string) as invoice_number,
                          cast(product_code as string) as product_code,
                          cast(standard_product_group_code as string) as standard_product_group_code,
                          cast(promotion_id as string) as promotion_id,
                          cast(IO as string) as IO,
                          cast(promotion_mechanism as string) as promotion_mechanism,
                          cast(promotion_desc as string) as promotion_desc,
                          cast(promotion_type as string) as promotion_type,
                          cast(budget_holder_group as string) as budget_holder_group,
                          cast(invoice_type as string) as invoice_type,
                          to_date(promo_start_date, 'dd.MM.yyyy') as promo_start_date,
                          to_date(promo_end_date, 'dd.MM.yyyy') as promo_end_date,
                          cast(value_based_promo_disc as double) as value_based_promo_disc,
                          cast(header_lvl_disc as double) as header_lvl_disc,
                          cast(free_quantity_value as double) as free_quantity_value,
                          cast(promotion_value as double) as promotion_value,
                          cast(distributor_code as string) as distributor_code,
                          cast(site_code as string) as site_code,
                          cast(outlet_code as string) as outlet_code,
                          cast(banded as string) as banded,
                          cast(gift_price_available as string) as gift_price_available,
                          cast(country as string) as country,
                          cast(year_id as int) as year_id,
                          cast(month_id as int) as month_id,
                          cast(time_key as int) as time_key
                         from final_nrm_data
                      """)

# COMMAND ----------

(
nrmDeltaDF.write
          .format("delta")
          .mode("append")
          .partitionBy("year_id", "month_id", "time_key")
          .save(deltaPathDailyPromotions)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC analyze table edge.fact_daily_promotions compute statistics;
# MAGIC optimize edge.fact_daily_promotions;
