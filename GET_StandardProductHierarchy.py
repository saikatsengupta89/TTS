# Databricks notebook source
articleMaster= "/mnt/adls/TTS/transformed/dimensions/dim_article_master"
localPH      = "/mnt/adls/EDGE_Analytics/Datalake/Transformed/Dimensions/Product"
localPHStg   = "/mnt/adls/EDGE_Analytics/Datalake/Staging/Incremental/Masters/Product_Hierarchy_Mapping"

# COMMAND ----------

artilceMasterDF= spark.read.parquet(articleMaster)
artilceMasterDF.createOrReplaceTempView("ref_article_master")

yearLPH = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(localPHStg)]))
monthLPH= str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(localPHStg +"/" +str(yearLPH))])).zfill(2)
dayLPH  = str(max([int(i.name.replace('/','')) for i in dbutils.fs.ls(localPHStg +"/"+str(yearLPH)+"/"+str(monthLPH))])).zfill(2)

localPHStgDF= spark.read \
                   .option("inferSchema",True) \
                   .option("header", True) \
                   .csv(localPHStg+"/"+yearLPH+"/"+monthLPH+"/"+dayLPH)
localPHStgDF.createOrReplaceTempView("stg_local_ph")

localPHDF= spark.sql ("""
                      select 
                        Material as product_code,
                        PH9 as PH9_code,
                        PH9_Desc as PH9_description,
                        PH7 as PH7_code,
                        `CD DP Name` as dp_name,
                        Portfolio as cotc,
                        Brand as brand,
                        Segment as segment,
                        `Small C` as category,
                        `Big C` as division
                      from stg_local_ph
                      """)

localPHDF.createOrReplaceTempView("dim_local_product")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop view if exists ref_standard_hierarchy;
# MAGIC create temporary view ref_standard_hierarchy as
# MAGIC with dataset_SPGC (
# MAGIC   select 
# MAGIC   distinct
# MAGIC   PH7_code as brand_code,
# MAGIC   product_code as standard_product_group_code
# MAGIC   from
# MAGIC   (select
# MAGIC       case when am.PH7_code is null then 'NA'
# MAGIC            else am.PH7_code 
# MAGIC       end PH7_code,
# MAGIC       lph.product_code,
# MAGIC       row_number() over (partition by am.PH7_code order by lph.product_code) RN
# MAGIC     from dim_local_product lph
# MAGIC     left outer join ref_article_master am 
# MAGIC     on am.material_code = lph.product_code
# MAGIC     where lph.dp_name != 'Not Assigned'
# MAGIC     --where upper(PH7_description) is not null    
# MAGIC   )
# MAGIC   where RN =1
# MAGIC )
# MAGIC select
# MAGIC product_code,
# MAGIC product_desc,
# MAGIC dp_name,
# MAGIC ph.brand_code,
# MAGIC ph.brand,
# MAGIC spgc.standard_product_group_code,
# MAGIC master_product_category,
# MAGIC master_market,
# MAGIC master_brand,
# MAGIC banded_PH8,
# MAGIC lead_basepack_code,
# MAGIC lead_basepack_desc,
# MAGIC category,
# MAGIC division,
# MAGIC case when (banded_flag + banded_PH8_level) > 0 then 'Banded'
# MAGIC      else 'Non-Banded' end as banded
# MAGIC from
# MAGIC (select 
# MAGIC     lph.product_code,
# MAGIC     nvl(am.material_desc, 'NA') as product_desc,
# MAGIC     case when lph.dp_name is null then 
# MAGIC          case when upper(am.PH9_description) = 'NAN' then 'NA' else nvl(am.PH9_description,'NA') end
# MAGIC          else lph.dp_name end as dp_name,
# MAGIC     case when upper(lph.cotc)='NON CORE' then 'Non-Core'
# MAGIC          when lph.cotc is null then 'Non-Core'
# MAGIC          else 'Core' end as cotc,
# MAGIC     nvl(am.PH7_code, 'NA') as brand_code,
# MAGIC     nvl(am.PH7_description, 'NA') as brand,
# MAGIC     nvl(am.PH3_description, 'NA') as master_product_category,
# MAGIC     nvl(am.PH4_description, 'NA') as master_market,
# MAGIC     nvl(am.PH5_description, 'NA') as master_brand,
# MAGIC     nvl(am.PH8_description, 'NA') as banded_PH8,
# MAGIC     nvl(lph.PH9_code, 'NA') as lead_basepack_code,
# MAGIC     nvl(lph.PH9_description, 'NA') as lead_basepack_desc,
# MAGIC     nvl(lph.category,'NA') category,
# MAGIC     nvl(lph.division,'NA') division,
# MAGIC     case when length(regexp_extract(am.material_desc, "\\([1-9]\\+[A-z0-9]",0))> 0 then 1 
# MAGIC          else 0 end banded_flag,
# MAGIC     case when length(regexp_extract(upper(am.PH8_description), 'BANDED',0))> 0 then 1 
# MAGIC          else 0 end banded_PH8_level
# MAGIC     from
# MAGIC     (select * 
# MAGIC       from dim_local_product 
# MAGIC       where dp_name != 'Not Assigned'
# MAGIC     ) lph
# MAGIC     left outer join ref_article_master am
# MAGIC     --inner join ref_article_master am
# MAGIC     on lph.product_code = am.material_code
# MAGIC ) ph
# MAGIC left outer join dataset_SPGC spgc on ph.brand_code= spgc.brand_code;

# COMMAND ----------

# CHECK QUERIES
# %sql
# select * from ref_article_master where material_code in (
# select product_code from dim_local_product where upper(dp_name)='NOT ASSIGNED'
# );

# %sql
# select nrm.product_code, promotion_type, count(1)
# from edge.processed_nrm_data nrm
# left outer join (select product_code from dim_local_product where upper(dp_name)!='NOT ASSIGNED') lph
# on nrm.product_code = lph.product_code
# where year_id=2019 and month_id=201901
# and lph.product_code is null
# --and nrm.on_off_flag=1
# group by nrm.product_code, promotion_type;

# %sql
# select nrm.product_code, count(1)
# from edge.processed_nrm_data nrm
# left outer join (select material_code from ref_article_master) am
# on nrm.product_code = am.material_code
# where year_id=2019 and month_id=201901
# and am.material_code is null
# group by nrm.product_code;
