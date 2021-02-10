# Databricks notebook source
# MAGIC %sql
# MAGIC refresh table edge.processed_nrm_data;

# COMMAND ----------

mappingOutlet      = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Outlet"
mappingChannel     = "/mnt/adls/TTS/transformed/dimensions/dim_channel"

mappingOutletDF = spark.read.option("inferSchema",True).option("header",True).csv(mappingOutlet)
mappingOutletDF.createOrReplaceTempView("dim_outlet")

mappingChannelDF = spark.read.parquet(mappingChannel)
mappingChannelDF.createOrReplaceTempView("dim_channel")

# COMMAND ----------

import pandas as pd
map_channel= {'PROFESSIONAL':'PROFESSIONAL',
              'MOM & POP':'MOM & POP',
              'F. GROCERY':'F. GROCERY',
              'WHOLESALER':'WHOLESALER',
              'DT H&B':'DT H&B',
              'MARKET STALL':'MARKET STALL'
}

mapChnPD = pd.DataFrame(list(map_channel.items()),columns = ['channel_org','channel'])
mapChnDF = spark.createDataFrame(mapChnPD)
mapChnDF.createOrReplaceTempView("ref_channel_map")

# COMMAND ----------

# MAGIC %sql
# MAGIC select promotion_type, promotion_mechanism, sum(promotion_value) promotion_value
# MAGIC from edge.processed_nrm_data
# MAGIC where month_id=202001
# MAGIC group by promotion_type, promotion_mechanism
# MAGIC order by 1,2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select promotion_type, promotion_mechanism, sum(promotion_value) promotion_value
# MAGIC from edge.fact_daily_promotions
# MAGIC where month_id=202001
# MAGIC group by promotion_type, promotion_mechanism
# MAGIC order by 1,2;
# MAGIC --125245018226.73833
# MAGIC --99961069536.00304

# COMMAND ----------

# MAGIC %sql
# MAGIC select promotion_type, promotion_mechanism, sum(promotion_value) promotion_value
# MAGIC from edge.fact_monthly_promo_agg
# MAGIC where month_id=202001
# MAGIC group by promotion_type, promotion_mechanism
# MAGIC order by 1,2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from edge.processed_nrm_data
# MAGIC where month_id=202001 and promotion_type='LTYPRG'
# MAGIC and invoice_no='H090535580';
# MAGIC --and IO='AD70048798';

# COMMAND ----------

# MAGIC %sql
# MAGIC select promotion_type, sum(promotion_value) from edge.fact_daily_promotions where month_id=202001 and invoice_number='D090813278'
# MAGIC group by promotion_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC select promotion_mechanism, promotion_type, count(1)
# MAGIC from edge.fact_daily_promotions where month_id=202001 and invoice_type='OFF'
# MAGIC and nvl(standard_product_group_code, 'NA') = 'NA'
# MAGIC group by promotion_mechanism, promotion_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC select promotion_mechanism, promotion_type, count(1)
# MAGIC from edge.fact_monthly_promo_agg where month_id=202001 and invoice_type='OFF'
# MAGIC and nvl(standard_product_group_code, 'NA') = 'NA'
# MAGIC group by promotion_mechanism, promotion_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC select invoice_no, product_code, promotion_value, channel
# MAGIC from edge.processed_nrm_data nrm
# MAGIC inner join 
# MAGIC (select 
# MAGIC     distinct
# MAGIC     outlet_code,
# MAGIC     nvl(map.channel, 'NA') channel
# MAGIC     from
# MAGIC     (select distinct
# MAGIC         ot.outlet_code,
# MAGIC         case when upper(chn.group_channel) ='PROFESSIONAL' then 'PROFESSIONAL'
# MAGIC              else chn.channel
# MAGIC              end channel
# MAGIC       from dim_outlet ot
# MAGIC       inner join dim_channel chn on chn.channel= ot.channel
# MAGIC     ) ot
# MAGIC     left join ref_channel_map map on ot.channel= map.channel_org
# MAGIC ) chn on nrm.outlet_code = chn.outlet_code
# MAGIC where promotion_type='PERSTR'
# MAGIC and invoice_no='D090813278';

# COMMAND ----------

processingYear="2020"
processingMonth="01"
yearMonth= int(processingYear+processingMonth)
quarter= 'Quarter 1'
year_id = processingYear
month_id= int(processingYear+processingMonth)

# COMMAND ----------

mappingPromo       = "/mnt/adls/TTS/transformed/dimensions/dim_promotion"
mappingIOMaster    = "/mnt/adls/TTS/transformed/facts/ref_mthly_io"
mappingChannel     = "/mnt/adls/TTS/transformed/dimensions/dim_channel"
mappingOutlet      = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Outlet"
promotionSplit     = "/mnt/adls/TTS/processed/references/ref_perstr_promotion_split/{}".format(processingYear)

# COMMAND ----------

mappingPromoDF= spark.read.parquet(mappingPromo)
mappingPromoDF.createOrReplaceTempView("dim_promotion")

mapIOMasterDF= spark.read.parquet(mappingIOMaster+"/"+processingYear+"/"+processingMonth)
mapIOMasterDF.createOrReplaceTempView("ref_mthly_io")

mappingChannelDF = spark.read.parquet(mappingChannel)
mappingChannelDF.createOrReplaceTempView("dim_channel")

mappingOutletDF = spark.read.option("inferSchema",True).option("header",True).csv(mappingOutlet)
mappingOutletDF.createOrReplaceTempView("dim_outlet")

promoSplitDF = spark.read.parquet(promotionSplit)
promoSplitDF.createOrReplaceTempView("ref_promotion_split")

# COMMAND ----------

# MAGIC %run ./GET_StandardProductHierarchy

# COMMAND ----------

map_channel= {'PROFESSIONAL':'PROFESSIONAL',
              'MOM & POP':'MOM & POP',
              'F. GROCERY':'F. GROCERY',
              'WHOLESALER':'WHOLESALER',
              'DT H&B':'DT H&B',
              'MARKET STALL':'MARKET STALL'
}

mapChnPD = pd.DataFrame(list(map_channel.items()),columns = ['channel_org','channel'])
mapChnDF = spark.createDataFrame(mapChnPD)
mapChnDF.createOrReplaceTempView("ref_channel_map")

# COMMAND ----------

offInvoicePromoDF= spark.sql("""select nrm.*, 
                                case when nrm.promotion_type='PERSTR' then 1 else 0 end perstr_flag,
                                chn.channel
                                from edge.processed_nrm_data nrm
                                inner join (
                                  select 
                                    distinct
                                    outlet_code,
                                    nvl(map.channel, 'NA') channel
                                    from
                                    (select distinct
                                        ot.outlet_code,
                                        case when upper(chn.group_channel) ='PROFESSIONAL' then 'PROFESSIONAL'
                                             else chn.channel
                                             end channel
                                      from dim_outlet ot
                                      inner join dim_channel chn on chn.channel= ot.channel
                                    ) ot
                                    left join ref_channel_map map on ot.channel= map.channel_org
                                ) chn on nrm.outlet_code = chn.outlet_code
                                inner join 
                                (select 
                                  promo_invoice,
                                  case when promotion_type ='BASKET' and promo_invoice='OFF' then 'BASKET_BD'
                                       else promotion_type 
                                  end as promotion_type
                                from dim_promotion) promo on nrm.promotion_type = promo.promotion_type
                                where year_id={} and month_id = {}
                                and promo.promo_invoice='OFF'
                                and nrm.on_off_flag = 1
                             """.format(year_id, month_id)
                            )

offInvoicePromoDF.createOrReplaceTempView("nrm_off_invoice_promo")

# COMMAND ----------

promoSplitDF= spark.sql("""
                        select 
                        upper(channel) channel,
                        upper(category) category,
                        case when total =0 then 0 else share/total end as percent
                        from
                        (select 
                          channel,
                          category, 
                          cast(proportion as double) as share,
                          sum(cast(proportion as double)) over (partition by channel) total
                          from (
                            select 
                            year,
                            quarter,
                            channel,
                            stack(9,'Fabsol',Fabsol, 
                                    'Fabsen',Fabsen, 
                                    'HNH',HNH, 
                                    'Hair',Hair, 
                                    'Skincare',Skincare, 
                                    'Skincleansing',Skincleansing, 
                                    'Oral',Oral, 
                                    'Savoury',Savoury, 
                                    'Tea',Tea
                            ) as (category, proportion) 
                            from
                            (select 
                              year,
                              quarter,
                              channel,
                              cast(Fabsol as string) as Fabsol,
                              cast(Fabsen as string) as Fabsen,
                              cast(HNH as string) as HNH,
                              cast(Hair as string) as Hair,
                              cast(Skincare as string) as Skincare,
                              cast(Skincleansing as string) as Skincleansing,
                              cast(Oral as string) as Oral,
                              cast(Savoury as string) as Savoury,
                              cast(Tea as string) as Tea
                              from ref_promotion_split 
                              where year='{}' and quarter='{}'
                             )
                           )
                         )""".format(processingYear, quarter)
                       )
promoSplitDF.createOrReplaceTempView("ref_channel_cat_share")

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   invoice_no, IO, product_code, promotion_value, nrm.channel,
# MAGIC   nvl(splt.percent,0) percent,
# MAGIC   splt.category
# MAGIC from (select * from nrm_off_invoice_promo where perstr_flag=1) nrm
# MAGIC left outer join ref_channel_cat_share splt 
# MAGIC on nrm.channel = splt.channel
# MAGIC where IO ='AB70029039' and invoice_no='D090813278';

# COMMAND ----------

offInvoicePromoPERSTRDF = spark.sql ("""
                                      select 
                                        nrm.invoice_no,
                                        nrm.product_code,
                                        io.standard_product_group_code,
                                        nrm.promotion_id,
                                        nrm.IO,                                        
                                        nrm.promotion_mechanism,
                                        nrm.promotion_desc,
                                        nrm.promotion_type,
                                        nrm.promo_start_date,
                                        nrm.promo_end_date,
                                        nrm.value_based_promo_disc,
                                        nrm.header_lvl_disc,
                                        nrm.free_quantity_value,
                                        nrm.promotion_value as promotion_value_vnd,
                                        nrm.distributor_code,
                                        nrm.site_code,
                                        nrm.outlet_code,
                                        io.master_product_category,
                                        io.master_market,
                                        nrm.country,
                                        nrm.year_id,
                                        nrm.month_id,
                                        nrm.time_key,
                                        nvl(io.category, 'NA') as category_IO,
                                        nvl(io.brand_code, 'NA') as brand_IO,
                                        nvl(io.budget_holder, 'NA') as budget_holder,
                                        nvl(io.budget_holder_grp, 'NA') as budget_holder_group,
                                        nvl(proportion_PERSTR, 0) proportion_PERSTR,
                                        nvl(nrm.percent,0) percent,
                                        nvl(proportion_PERSTR, 0) * nvl(nrm.percent,0) as proportion,
                                        nvl(promotion_value,0) * nvl(proportion_PERSTR, 0) * nvl(nrm.percent,0) promotion_value,
                                        nvl(promotion_value,0) as old_promotion_value
                                      from 
                                      (select 
                                          nrm.invoice_no,
                                          nrm.product_code,
                                          nrm.promotion_id,
                                          nrm.IO,                                        
                                          nrm.promotion_mechanism,
                                          nrm.promotion_desc,
                                          nrm.promotion_type,
                                          nrm.promo_start_date,
                                          nrm.promo_end_date,
                                          nrm.value_based_promo_disc,
                                          nrm.header_lvl_disc,
                                          nrm.free_quantity_value,
                                          nrm.promotion_value,
                                          nrm.distributor_code,
                                          nrm.site_code,
                                          nrm.outlet_code,
                                          nrm.distributor_code,
                                          nrm.channel,
                                          nrm.site_code,
                                          nrm.outlet_code,
                                          nrm.country,
                                          nrm.year_id,
                                          nrm.month_id,
                                          nrm.time_key,
                                          nvl(splt.percent,0) percent,
                                          splt.category
                                        from (select * from nrm_off_invoice_promo where perstr_flag=1) nrm
                                        left outer join ref_channel_cat_share splt 
                                        on nrm.channel = splt.channel
                                      ) nrm
                                      left outer join 
                                      (select 
                                        io.*,
                                        ph.standard_product_group_code,
                                        ph.master_product_category,
                                        ph.master_market
                                        from ref_mthly_io io
                                        left join 
                                        (select 
                                          distinct
                                          brand_code,
                                          standard_product_group_code,
                                          master_product_category,
                                          master_market
                                          from ref_standard_hierarchy
                                        ) ph on io.brand_code = ph.brand_code
                                      ) io on nrm.IO= io.IO and upper(nrm.category) = upper(io.category)
                                      """)
# display(offInvoicePromoPERSTRDF)
offInvoicePromoPERSTRDF.createOrReplaceTempView("perstr_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ref_mthly_io where IO='AB70029039';

# COMMAND ----------

# MAGIC %sql
# MAGIC select io.IO,
# MAGIC io.brand_code,
# MAGIC ph.standard_product_group_code,
# MAGIC io.category,
# MAGIC io.proportion_PERSTR
# MAGIC from ref_mthly_io io
# MAGIC left join 
# MAGIC (select 
# MAGIC   distinct
# MAGIC   brand_code,
# MAGIC   standard_product_group_code,
# MAGIC   master_product_category,
# MAGIC   master_market
# MAGIC   from ref_standard_hierarchy
# MAGIC ) ph on io.brand_code = ph.brand_code
# MAGIC where IO='AB70029039';

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC distinct
# MAGIC io.IO,
# MAGIC io.category,
# MAGIC io.proportion_PERSTR,
# MAGIC io.budget_holder,
# MAGIC io.budget_holder_grp,
# MAGIC ph.standard_product_group_code,
# MAGIC ph.master_product_category,
# MAGIC ph.master_market
# MAGIC from ref_mthly_io io
# MAGIC left join 
# MAGIC (select 
# MAGIC   distinct
# MAGIC   brand_code,
# MAGIC   standard_product_group_code,
# MAGIC   master_product_category,
# MAGIC   master_market
# MAGIC   from ref_standard_hierarchy
# MAGIC ) ph on io.brand_code = ph.brand_code
# MAGIC where IO='AB70029039';
