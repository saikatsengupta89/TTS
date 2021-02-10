# Databricks notebook source
import pandas as pd

# COMMAND ----------

pathDimChannel= "/mnt/adls/TTS/transformed/dimensions/dim_channel"
pathDimPromotion= "/mnt/adls/TTS/transformed/dimensions/dim_promotion"
pathMappingStoreSize= "/mnt/adls/TTS/processed/masters/mapping_store_size"
pathMappingBudgetHolder= "/mnt/adls/TTS/processed/masters/mapping_budget_holder"
pathDimDistributor= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Distributor"
pathDimOutlet= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Outlet"
pathDimTime= "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Time"
pathDimPromoAll = "/mnt/adls/TTS/transformed/dimensions/dim_promo_all"
pathDimPromoSummarised = "/mnt/adls/TTS/transformed/dimensions/dim_promo_summarised"

pathDimCodedConsolidated= "/mnt/adls/TTS/transformed/dimensions/dim_coded_consolidated"
pathDimDynamicAxis = "/mnt/adls/TTS/transformed/dimensions/dim_dynamic_axis"
pathDimPromoFlexible = "/mnt/adls/TTS/transformed/dimensions/dim_promo_flexible"

# COMMAND ----------

yearTD = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimDistributor)]))
monthTD= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimDistributor +"/" +str(yearTD))])).zfill(2)
dayTD  = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimDistributor +"/"+str(yearTD)+"/"+str(monthTD))])).zfill(2)

yearDT = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimTime)]))
monthDT= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimTime +"/" +str(yearDT))])).zfill(2)
dayDT  = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls (pathDimTime +"/"+str(yearDT)+"/"+str(monthDT))])).zfill(2)

distributorDF= spark.read.option("header","true") \
                         .option("inferSchema","true") \
                         .csv(pathDimDistributor+"/"+yearTD+"/"+monthTD+"/"+dayTD)

outletDF= spark.read.option("header","true") \
                         .option("inferSchema","true") \
                         .csv(pathDimOutlet)

dimTimeDF= spark.read.option("header","true") \
                     .option("inferSchema","true") \
                     .csv(pathDimTime+"/"+yearDT+"/"+monthDT+"/"+dayDT)

dimPromotionDF= spark.read.parquet(pathDimPromotion)

dimPromoAllDF = spark.read.parquet(pathDimPromoAll)

dimPromoSummDF = spark.read.parquet(pathDimPromoSummarised)

mappingStoreSizeDF= spark.read.parquet(pathMappingStoreSize)

mappingChannelDF = spark.read.parquet(pathDimChannel)

mappingBudgetHolderDF = spark.read.parquet(pathMappingBudgetHolder)

outletDF.createOrReplaceTempView ("dim_outlet")
distributorDF.createOrReplaceTempView ("dim_distributor")
dimPromotionDF.createOrReplaceTempView("dim_promotion")
dimTimeDF.createOrReplaceTempView("dim_time")
dimPromoAllDF.createOrReplaceTempView("dim_promo_all")
dimPromoSummDF.createOrReplaceTempView("dim_promo_summarised")
mappingChannelDF.createOrReplaceTempView("dim_channel")
mappingStoreSizeDF.createOrReplaceTempView("mapping_store_size")
mappingBudgetHolderDF.createOrReplaceTempView("mapping_budget_holder")

# COMMAND ----------

# MAGIC %run ./GET_StandardProductHierarchy

# COMMAND ----------

map_axis= { 'Region': 1,
            'Channel': 2,
            'Store Size': 3,
            'Product Category': 4
}
mapAxisPD = pd.DataFrame(list(map_axis.items()),columns = ['axis_dimension','axis_dimension_index'])
mapAxisDF = spark.createDataFrame(mapAxisPD)
mapAxisDF.createOrReplaceTempView("axis_coded")

map_promo= {
             'DETAILED': 1,
             'SUMMARIZED': 2,
             'BUDGET HOLDER': 3
}
mapPromoPD = pd.DataFrame(list(map_promo.items()),columns = ['axis_dimension','dimension_index'])
mapPromoDF = spark.createDataFrame(mapPromoPD)
mapPromoDF.createOrReplaceTempView("axis_promo")

# COMMAND ----------

mapPerfectStore = {
     'YES': 1,
     'NO': 0
}
mapPerfectStorePD = pd.DataFrame(list(mapPerfectStore.items()),columns = ['perfect_store_status','perfect_store_status_code'])
mapPerfectStoreDF = spark.createDataFrame(mapPerfectStorePD)
mapPerfectStoreDF.createOrReplaceTempView("coded_perfect_store")
#display(mapPerfectStoreDF)

# COMMAND ----------

codeChannelDF = spark.sql ("""
                            select 
                            channel,
                            row_number() over(order by channel) channel_code,
                            10+ row_number() over(order by channel) channel_index
                            from (
                              select distinct 
                                case when group_channel='PROFESSIONAL' then group_channel else channel end as channel
                              from dim_channel 
                              where group_channel !='IC'
                              order by 1
                            ) chn
                            union all
                            select 'ALL' channel, 100000 channel_code, 100000 channel_index
                          """)
codeChannelDF.createOrReplaceTempView("coded_channel")
#display(codeChannelDF)

# COMMAND ----------

codeCategoryDF = spark.sql ("""
                            select 
                             category,
                             row_number() over(order by category) category_code,
                             30+ row_number() over(order by category) category_index
                            from
                            (select 
                                distinct category
                              from dim_local_product
                              where upper(dp_name) != 'NOT ASSIGNED'
                              order by category
                            )
                            union all
                            select 'ALL' category, 100000 category_code, 100000 category_index
                            """)
codeCategoryDF.createOrReplaceTempView("coded_category")
#display(codeCategoryDF)

# COMMAND ----------

map_region= { 'CENTRAL': 1,
              'HO CHI MINH - EAST': 2,
              'MEKONG DELTA': 3,
              'NORTH': 4
}

mapRegionPD = pd.DataFrame(list(map_region.items()),columns = ['region','region_index'])
mapRegionIntDF = spark.createDataFrame(mapRegionPD)
mapRegionIntDF.createOrReplaceTempView("coded_region_int")

mapRegionDF = spark.sql("""
                         select
                           region,
                           region_index region_code,
                           region_index
                         from coded_region_int
                         union all
                         select 'ALL' region, 100000 region_code, 100000 region_index
                        """)
mapRegionDF.createOrReplaceTempView("coded_region")
#display(mapRegionDF)

# COMMAND ----------

codedPerfectStoreDF =spark.sql("""select 
                                   ref_desc as store_size_class,
                                   ref_code as store_size_code,
                                   20+ ref_code as store_size_index
                                  from mapping_store_size where upper(ref_type)='STORE_SIZE_CLASS'
                                  union all
                                  select 'ALL' store_size_class, 100000 store_size_code, 100000 store_size_index
                               """)
codedPerfectStoreDF.createOrReplaceTempView("coded_store_size")
#display(codedPerfectStoreDF)

# COMMAND ----------

codedBudgetHolderGrpDF=spark.sql("""
                                  select
                                  budget_holder_grp,
                                  row_number() over(order by budget_holder_grp) budget_holder_grp_code
                                  from 
                                  (select distinct budget_holder_grp from mapping_budget_holder
                                  order by budget_holder_grp
                                  )
                                  union all
                                  select 'Distributor' budget_holder_grp, 99 budget_holder_grp_code""")
codedBudgetHolderGrpDF.createOrReplaceTempView('coded_budget_holder')
#display(codedBudgetHolderGrpDF)

# COMMAND ----------

codedConsolidatedDF = spark.sql("""
                                 select 
                                    ref_type,
                                    channel as ref_desc,
                                    channel_code as ref_code,
                                    null param1,
                                    null param2
                                   from
                                   (select 'CHANNEL' as ref_type, chn.channel, chn.channel_code from coded_channel chn where channel != 'ALL'
                                     union all
                                     select 'REGION' as ref_type, reg.region, reg.region_code from coded_region reg where region != 'ALL'
                                     union all
                                     select 'CATEGORY' as ref_type, cat.category, cat.category_code from coded_category cat where category != 'ALL'
                                     union all
                                     select 'PERFECT_STORE' as ref_type, perfect_store_status, perfect_store_status_code from coded_perfect_store
                                     union all
                                     select 'BUDGET_HOLDER_GRP' as ref_type, budget_holder_grp, budget_holder_grp_code from coded_budget_holder
                                   )
                                   union all
                                   select 
                                     'STORE_SIZE_CLASS' as ref_type,
                                     ref_desc,
                                     ref_code,
                                     param1,
                                     param2
                                   from mapping_store_size where upper(ref_type)='STORE_SIZE_CLASS'
                                """)
#display(codedConsolidatedDF)

# COMMAND ----------

dynamicAxisDF = spark.sql ("""
                           select 
                            axis_dimension,
                            cast(axis_dimension_index as int) axis_dimension_index,
                            region,
                            cast(region_index as int) region_index,
                            channel,
                            cast(channel_index as int) channel_index,
                            store_size_class,
                            cast(store_size_index as int) store_size_index,
                            category,
                            cast(region_index as int) category_index,
                            case when upper(axis_dimension) = 'REGION' then region
                                 when upper(axis_dimension) = 'CHANNEL' then channel
                                 when upper(axis_dimension) = 'STORE SIZE' then store_size_class
                                 when upper(axis_dimension) = 'PRODUCT CATEGORY' then category
                                 end axis_value,
                            cast(
                            case when upper(axis_dimension) = 'REGION' then region_index
                                 when upper(axis_dimension) = 'CHANNEL' then channel_index
                                 when upper(axis_dimension) = 'STORE SIZE' then store_size_index
                                 when upper(axis_dimension) = 'PRODUCT CATEGORY' then category_index
                                 end as int) axis_index
                            from 
                            axis_coded,
                            (select region, region_index from coded_region),
                            (select channel, channel_index from coded_channel),
                            (select store_size_class, store_size_index from coded_store_size),
                            (select category, category_index from coded_category)
                            """)

# COMMAND ----------

promoDetailDF=spark.sql("""
                        select promo_type promo_detailed, cast(index as int) promo_detailed_index
                        from dim_promo_all where group='Detailed'
                        """)
promoDetailDF.createOrReplaceTempView("coded_promo_detailed")

promoSummaryDF = spark.sql("""
                           select distinct promo_summarized promo_summarised, cast(summarized_index as int) promo_summarised_index
                           from dim_promotion
                           """)
promoSummaryDF.createOrReplaceTempView("coded_promo_summary")

promoBudgetHolderDF = spark.sql("""
                                select 
                                budget_holder_grp,
                                30 +row_number() over(order by budget_holder_grp ) budget_holder_index
                                from (
                                 select distinct budget_holder_grp
                                 from coded_budget_holder
                                )
                                """)
promoBudgetHolderDF.createOrReplaceTempView("coded_promo_budget_holder")

# COMMAND ----------

promoFlexibleDF = spark.sql ("""
                             select
                              axis_promo.axis_dimension,
                              axis_promo.dimension_index,
                              case when axis_dimension = 'DETAILED' then coded_promo_detailed.promo_detailed
                                   when axis_dimension = 'SUMMARIZED' then coded_promo_summary.promo_summarised
                                   when axis_dimension = 'BUDGET HOLDER' then coded_promo_budget_holder.budget_holder_grp
                                   end axis_value,
                              case when axis_dimension = 'DETAILED' then coded_promo_detailed.promo_detailed_index
                                   when axis_dimension = 'SUMMARIZED' then coded_promo_summary.promo_summarised_index
                                   when axis_dimension = 'BUDGET HOLDER' then coded_promo_budget_holder.budget_holder_index
                                   end axis_index,
                              coded_promo_summary.promo_summarised,
                              coded_promo_detailed.promo_detailed as promotion_type,
                              coded_promo_budget_holder.budget_holder_grp as budget_holder
                              from 
                              axis_promo,
                              coded_promo_detailed,
                              coded_promo_summary,
                              coded_promo_budget_holder
                              """)

# COMMAND ----------

# WRITING THE CUSTOMIZED DIMENSIONS FOR TTS REPORTING
(
codedConsolidatedDF.repartition(1)
                   .write
                   .mode("overwrite")
                   .parquet(pathDimCodedConsolidated)
)

(
dynamicAxisDF.repartition(1)
             .write
             .mode("overwrite")
             .parquet(pathDimDynamicAxis)
)

(
promoFlexibleDF.repartition(1)
               .write
               .mode("overwrite")
               .parquet(pathDimPromoFlexible)
)
