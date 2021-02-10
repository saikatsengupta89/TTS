# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import col

def getQuarter(month):
  if month in ['01','02','03']:
    return 'Quarter 1'
  elif month in ['04','05','06']:
    return 'Quarter 2'
  elif month in ['07','08','09']:
    return 'Quarter 3'
  else:
    return 'Quarter 4'

processingYear  =dbutils.widgets.get("ProcessingYear")
processingMonth =dbutils.widgets.get("ProcessingMonth")
yearMonth= int(processingYear+processingMonth)
quarter= getQuarter(processingMonth)
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
#303713
#543236

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
                                        nvl(proportion_PERSTR, 0) * nvl(nrm.percent,0) as proportion,
                                        nvl(promotion_value,0) * nvl(proportion_PERSTR, 0) * nvl(nrm.percent,0) promotion_value
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
                                      (select io.*,
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

offInvoicePromoNonPERSTRDF = spark.sql("""
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
                                          nvl(proportion, 0) as proportion,
                                          nvl(promotion_value,0) * nvl(proportion, 0) promotion_value
                                        from (select * from nrm_off_invoice_promo where perstr_flag=0) nrm
                                        left outer join
                                        (select io.*,
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
                                        ) io on nrm.IO= io.IO 
                                    """)
offInvoicePromoNonPERSTRDF.createOrReplaceTempView("non_perstr_data")
# display(offInvoicePromoNonPERSTRDF)

# COMMAND ----------

offInvoicePromoDF = offInvoicePromoPERSTRDF.unionAll(offInvoicePromoNonPERSTRDF)
offInvoicePromoDF.createOrReplaceTempView("nrm_off_invoice")
# display(spark.sql("select promotion_type, count(1) cnt from nrm_off_invoice group by promotion_type"))

# COMMAND ----------

nrmOffInvoiceData= spark.sql ("""
                               select 
                                  invoice_no,
                                  product_code,
                                  standard_product_group_code,
                                  promotion_id,
                                  IO,
                                  promotion_mechanism,
                                  promotion_desc,
                                  promotion_type,
                                  (case
                                    when budget_holder_group is null and promotion_type='DTRDIS' then 'Distributor'
                                    when budget_holder_group is null then 'Trade Category'
                                    else budget_holder_group
                                  end) as budget_holder_group,
                                  'OFF' invoice_type,
                                  promo_start_date,
                                  promo_end_date,
                                  case when nvl(prop_value_based,0) = 0 then value_based_promo_disc else prop_value_based end as value_based_promo_disc,
                                  case when nvl(prop_header_level,0) = 0 then header_lvl_disc else prop_header_level end as header_lvl_disc,
                                  case when nvl(prop_free_quantity,0) = 0 then free_quantity_value else prop_free_quantity end as free_quantity_value,
                                  nvl(promotion_value,0) promotion_value,
                                  distributor_code,
                                  site_code,
                                  outlet_code,
                                  'Non-Banded' banded,
                                  country,
                                  year_id,
                                  month_id,
                                  time_key
                                from
                                (select
                                  nrm.*,
                                  nvl(nrm.value_based_promo_disc * nrm.proportion, 0) prop_value_based,
                                  nvl(nrm.header_lvl_disc * nrm.proportion, 0) prop_header_level,
                                  nvl(nrm.free_quantity_value * nrm.proportion, 0) prop_free_quantity
                                  from nrm_off_invoice nrm
                                ) nrm
                            """)
nrmOffInvoiceData.createOrReplaceTempView("nrm_off_invoice_final")

# COMMAND ----------

# CHECK QUERIES
# %sql
# select 
# distinct
# io.brand_code
# from ref_mthly_io io
# left join 
# (select 
#   distinct
#   PH7_code brand_code,
#   standard_product_group_code,
#   master_product_category,
#   master_market
#   from ref_standard_hierarchy
# ) ph on io.brand_code = ph.brand_code
# where ph.brand_code is null;

# %sql
# select distinct nrm.IO, io.IO 
# from nrm_off_invoice_promo nrm
# left outer join ref_mthly_io io on nrm.IO= io.IO
# where io.IO is null;

# select IO, sum(case when standard_product_group_code is null then 0 else 1 end) cnt
# from (
# select io.*,
# ph.standard_product_group_code,
# ph.master_product_category,
# ph.master_market
# from ref_mthly_io io
# left join 
# (select 
#   distinct
#   PH7_code brand_code,
#   standard_product_group_code,
#   master_product_category,
#   master_market
#   from ref_standard_hierarchy
# ) ph on io.brand_code = ph.brand_code 
# where IO in ('AB70049039',
# 'BD70058147',
# 'AB70038416',
# 'AB70028990',
# 'AB70044046',
# 'BB70028988',
# 'BD70039124',
# 'AB70029039',
# 'AB70037099',
# 'AD70048156',
# 'AB70029824',
# 'AB70028994',
# 'BD70053969',
# 'BB70031779',
# 'BD70062920',
# 'AB70029819',
# 'AB70031779',
# 'AB70054839',
# 'AB70029794',
# 'AB70034127',
# 'AD70048798',
# 'AB70044437',
# 'AB70029255',
# 'AD70039734',
# 'AB70029000',
# 'AB70049136',
# 'AO70028484',
# 'AB70032364',
# 'AB70028997',
# 'BB70063538',
# 'AD70055634',
# 'AD70052742',
# 'AB70050375',
# 'BD70046467',
# 'BD70053279',
# 'BD70050618',
# 'BD70050435',
# 'BD70046478',
# 'BD70053281'))
# group by IO;

# %sql
# select io.*,
# ph.standard_product_group_code,
# ph.master_product_category,
# ph.master_market
# from ref_mthly_io io
# left join 
# (select 
#   distinct
#   PH7_code brand_code,
#   standard_product_group_code,
#   master_product_category,
#   master_market
#   from ref_standard_hierarchy
# ) ph on io.brand_code = ph.brand_code 
# where IO ='AB70029039'
