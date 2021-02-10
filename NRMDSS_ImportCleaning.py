# Databricks notebook source
import pandas as pd
import numpy as np
import pyspark.sql.functions as f
from pyspark.sql import types
from typing import List

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
processingYear  =dbutils.widgets.get("ProcessingYear")
processingMonth =dbutils.widgets.get("ProcessingMonth")
validationThreshold =float(dbutils.widgets.get("ValidationThreshold"))
yearMonth= int(processingYear+processingMonth)
print(yearMonth)

# dbutils.widgets.text("ProcessingYear","2020")
# dbutils.widgets.text("ProcessingMonth","02")
# dbutils.widgets.text("ValidationThreshold", "50")

# COMMAND ----------

def convertToFloat(x):
  x= str(x).replace(',','')
  if (x.find('-')> 0):
    x= float(x.replace('-','')) * -1
  else:
    x= float(x)
  return x

def optimize_floats(df: pd.DataFrame) -> pd.DataFrame:
    floats = df.select_dtypes(include=['float64']).columns.tolist()
    df[floats] = df[floats].apply(pd.to_numeric, downcast='float')
    return df


def optimize_ints(df: pd.DataFrame) -> pd.DataFrame:
    ints = df.select_dtypes(include=['int64']).columns.tolist()
    df[ints] = df[ints].apply(pd.to_numeric, downcast='integer')
    return df


def optimize_objects(df: pd.DataFrame, datetime_features: List[str]) -> pd.DataFrame:
    for col in df.select_dtypes(include=['object']):
        if col not in datetime_features:
            num_unique_values = len(df[col].unique())
            num_total_values = len(df[col])
            if float(num_unique_values) / num_total_values < 0.5:
                df[col] = df[col].astype('str')
        else:
            df[col] = pd.to_datetime(df[col])
    return df


def optimize(df: pd.DataFrame, datetime_features: List[str] = []):
    return optimize_floats(optimize_ints(optimize_objects(df, datetime_features)))

# COMMAND ----------

# MAGIC %run /Shared/TTS/GET_ProcessingMthTradingData

# COMMAND ----------

# define the file paths here
csvParentPath= f"/mnt/adls/TTS/raw/daily/nrm_dss/{yearMonth}/"

# COMMAND ----------

fileInfo = dbutils.fs.ls (csvParentPath)
fileList= [str(i.name) for i in fileInfo]
fileListFiltered= list(filter(lambda x: x.__contains__("csv"), fileList))
#print(fileListFiltered)
print(len(fileListFiltered))

totalRows=0
npArrayList = []
for fileName in fileListFiltered:
  #if fileName=="LE VN NRM DSS G1 20190101 V18.csv":
    #continue
  path= f"/dbfs/mnt/adls/TTS/raw/daily/nrm_dss/{yearMonth}/{fileName}"
  subset= pd.read_csv(filepath_or_buffer=path, 
                      index_col=None,
                      skiprows=5, 
                      header=0,
                      delimiter=';',
                      dtype='object')
  #print(fileName)
  #print(subset.head(1))
  npArrayList.append(subset)
  totalRows += int(len(subset.index))

combNpArray = np.vstack(npArrayList)
nrmRaw=pd.DataFrame(combNpArray)

# COMMAND ----------

nrmRawCount = int(len(nrmRaw.index))

if (totalRows == nrmRawCount):
  print("Count Matched")
  print("Total consolidated rows: ", nrmRawCount)
  print("Total rows combining all files: ", totalRows)
else:
  print("Exception: Row count not matching after consolidation with the total rows aggregated over all the file")
  print("Total consolidated rows: ", nrmRawCount)
  print("Total rows combining all files: ", totalRows)

# COMMAND ----------

# assigning header values to the pandas dataframe
colNames= [
  "country", "calendar_year", "calendar_month", "calendar_day", "distributor", "site", "outlet", "billing_document", "billing_type", "billing_item",
  "product", "promotion_id", "promotion_desc1", "promo_start_date", "promo_end_date", "promotion_type", "value_based_promo_disc",
  "header_lvl_disc", "free_qty_in_cs", "free_qty_in_pc", "free_qty_val_in_cs", "free_qty_val_in_pc", "free_qty_retail_price_pc",
  "free_qty_retail_price_cs"
]
nrmRaw.columns = colNames

# remove unwanted rows coming from column headers 
nrmRaw=nrmRaw.loc[nrmRaw["country"]!="Country"].copy(deep=True)

# COMMAND ----------

# provide proper data types to each series in the dataframe
# do all the string conversion to float for all measure fields
nrmRaw['value_based_promo_disc']= nrmRaw['value_based_promo_disc'].apply(convertToFloat)
nrmRaw['header_lvl_disc']=  nrmRaw['header_lvl_disc'].apply(convertToFloat)
nrmRaw['free_qty_in_cs']=  nrmRaw['free_qty_in_cs'].apply(convertToFloat)
nrmRaw['free_qty_in_pc']=  nrmRaw['free_qty_in_pc'].apply(convertToFloat)
nrmRaw['free_qty_val_in_cs']=  nrmRaw['free_qty_val_in_cs'].apply(convertToFloat)
nrmRaw['free_qty_val_in_pc']=  nrmRaw['free_qty_val_in_pc'].apply(convertToFloat)
nrmRaw['free_qty_retail_price_pc']= nrmRaw['free_qty_retail_price_pc'].apply(convertToFloat)
nrmRaw['free_qty_retail_price_cs']= nrmRaw['free_qty_retail_price_cs'].apply(convertToFloat)

nrmRaw = optimize(nrmRaw)
totalRows = len(nrmRaw.index)

# COMMAND ----------

nrmRaw.groupby(['promotion_type']).size()

# COMMAND ----------

#print(nrmRaw[(nrmRaw["site"]=="3004") & (nrmRaw["calendar_day"]=="28.01.2019")]["value_based_promo_disc"].sum())
# print(nrmRaw[(nrmRaw["site"]=="3040") & (nrmRaw["calendar_day"]=="01.01.2019")]["value_based_promo_disc"].sum())
# print(nrmRaw[(nrmRaw["site"]=="3040") & (nrmRaw["calendar_day"]=="01.01.2019")]["header_lvl_disc"].sum())
# print(nrmRaw[(nrmRaw["site"]=="3040") & (nrmRaw["calendar_day"]=="01.01.2019")]["free_qty_in_cs"].sum())
# print(nrmRaw[(nrmRaw["site"]=="3040") & (nrmRaw["calendar_day"]=="01.01.2019")]["free_qty_in_pc"].sum())

# COMMAND ----------

oth_list=['DTRDIS','LINDIS','BASKET','VOLDIS']
nrmRawDTRDIS= nrmRaw[nrmRaw['promotion_type']=='DTRDIS']
nrmRawLINDIS= nrmRaw[nrmRaw['promotion_type']=='LINDIS']
nrmRawBASKET= nrmRaw[nrmRaw['promotion_type']=='BASKET']
nrmRawVOLDIS= nrmRaw[nrmRaw['promotion_type']=='VOLDIS']
nrmRawOTH   = nrmRaw[~nrmRaw['promotion_type'].isin(oth_list)]

total_subset = len(nrmRawDTRDIS.index)+ len(nrmRawLINDIS.index) + len(nrmRawBASKET.index) + \
               len(nrmRawVOLDIS.index) + len(nrmRawOTH.index)
try:
  if (totalRows == total_subset):
    del nrmRaw
    print("nrmRaw dataframe removed")
  else:
    raise Exception ("The total raw count of NRM data doesn't matches with the subset. Can't proceed further")
except Exception as e:
  raise dbutils.notebook.exit(e)

# COMMAND ----------

# print(nrmRawDTRDIS[(nrmRawDTRDIS["site"]=="3004") & (nrmRawDTRDIS["calendar_day"]=="28.01.2019")]["value_based_promo_disc"].sum())
# print(nrmRawLINDIS[(nrmRawLINDIS["site"]=="3004") & (nrmRawLINDIS["calendar_day"]=="28.01.2019")]["value_based_promo_disc"].sum())
# print(nrmRawBASKET[(nrmRawBASKET["site"]=="3004") & (nrmRawBASKET["calendar_day"]=="28.01.2019")]["value_based_promo_disc"].sum())
# print(nrmRawVOLDIS[(nrmRawVOLDIS["site"]=="3004") & (nrmRawVOLDIS["calendar_day"]=="28.01.2019")]["value_based_promo_disc"].sum())
# print(nrmRawOTH[(nrmRawOTH["site"]=="3004") & (nrmRawOTH["calendar_day"]=="28.01.2019")]["value_based_promo_disc"].sum())

# COMMAND ----------

# print(nrmRawDTRDIS[(nrmRawDTRDIS["site"]=="3004") & (nrmRawDTRDIS["calendar_day"]=="28.01.2019")]["value_based_promo_disc"].sum() + nrmRawLINDIS[(nrmRawLINDIS["site"]=="3004") & (nrmRawLINDIS["calendar_day"]=="28.01.2019")]["value_based_promo_disc"].sum() +
# nrmRawBASKET[(nrmRawBASKET["site"]=="3004") & (nrmRawBASKET["calendar_day"]=="28.01.2019")]["value_based_promo_disc"].sum() +
# nrmRawVOLDIS[(nrmRawVOLDIS["site"]=="3004") & (nrmRawVOLDIS["calendar_day"]=="28.01.2019")]["value_based_promo_disc"].sum() +
# nrmRawOTH[(nrmRawOTH["site"]=="3004") & (nrmRawOTH["calendar_day"]=="28.01.2019")]["value_based_promo_disc"].sum())

# COMMAND ----------

print("Total Partitioned Memory footprint NRM DF: ")
print("nrmRawDTRDIS :"+ str(round((nrmRawDTRDIS.memory_usage(index=True).sum())/(1024*1024*1024),5)) +" GB")
print("nrmRawLINDIS :"+ str(round((nrmRawLINDIS.memory_usage(index=True).sum())/(1024*1024*1024),5)) +" GB")
print("nrmRawBASKET :"+ str(round((nrmRawBASKET.memory_usage(index=True).sum())/(1024*1024*1024),5)) +" GB")
print("nrmRawVOLDIS :"+ str(round((nrmRawVOLDIS.memory_usage(index=True).sum())/(1024*1024*1024),5)) +" GB")
print("nrmRawOTH    :"+ str(round((nrmRawOTH.memory_usage(index=True).sum())/(1024*1024*1024),5)) +" GB")

# COMMAND ----------

# MAGIC %run /Shared/TTS/GET_SiteAndDistributorDT

# COMMAND ----------

nrmDssSchema= types.StructType([
  types.StructField("country", types.StringType()),
  types.StructField("calendar_year", types.StringType()),
  types.StructField("calendar_month", types.StringType()),
  types.StructField("calendar_day", types.StringType()),
  types.StructField("distributor", types.StringType()),
  types.StructField("site", types.StringType()),
  types.StructField("outlet", types.StringType()),
  types.StructField("billing_document", types.StringType()),
  types.StructField("billing_type", types.StringType()),
  types.StructField("billing_item", types.StringType()),
  types.StructField("product", types.StringType()),
  types.StructField("promotion_id", types.StringType()),
  types.StructField("promotion_desc1", types.StringType()),
  types.StructField("promo_start_date", types.StringType()),
  types.StructField("promo_end_date", types.StringType()),
  types.StructField("promotion_type", types.StringType()),
  types.StructField("value_based_promo_disc", types.DoubleType()),
  types.StructField("header_lvl_disc", types.DoubleType()),
  types.StructField("free_qty_in_cs", types.DoubleType()),
  types.StructField("free_qty_in_pc", types.DoubleType()),
  types.StructField("free_qty_val_in_cs", types.DoubleType()),
  types.StructField("free_qty_val_in_pc", types.DoubleType()),
  types.StructField("free_qty_retail_price_pc", types.DoubleType()),
  types.StructField("free_qty_retail_price_cs", types.DoubleType())
])

nrmRawDTRDISDF= spark.createDataFrame(nrmRawDTRDIS, schema=nrmDssSchema)
nrmRawDTRDISDF.createOrReplaceTempView("raw_nrm_data_dtrdis")

nrmRawLINDISDF= spark.createDataFrame(nrmRawLINDIS, schema=nrmDssSchema)
nrmRawLINDISDF.createOrReplaceTempView("raw_nrm_data_lindis")

nrmRawBASKETDF= spark.createDataFrame(nrmRawBASKET, schema=nrmDssSchema)
nrmRawBASKETDF.createOrReplaceTempView("raw_nrm_data_basket")

nrmRawVOLDISDF= spark.createDataFrame(nrmRawVOLDIS, schema=nrmDssSchema)
nrmRawVOLDISDF.createOrReplaceTempView("raw_nrm_data_voldis")

nrmRawOTHDF= spark.createDataFrame(nrmRawOTH, schema=nrmDssSchema)
nrmRawOTHDF.createOrReplaceTempView("raw_nrm_data_oth")

# COMMAND ----------

# %sql
# /*
# 25303564.0
# 145699900.0
# 161838670.0
# 24651172.0
# 30750000.0
# */
# select sum(value_based_promo_disc) from raw_nrm_data_dtrdis where calendar_day='28.01.2019' and site='3004'
# union all
# select sum(value_based_promo_disc) from raw_nrm_data_lindis where calendar_day='28.01.2019' and site='3004'
# union all
# select sum(value_based_promo_disc) from raw_nrm_data_basket where calendar_day='28.01.2019' and site='3004'
# union all
# select sum(value_based_promo_disc) from raw_nrm_data_voldis where calendar_day='28.01.2019' and site='3004'
# union all
# select sum(value_based_promo_disc) from raw_nrm_data_oth where calendar_day='28.01.2019' and site='3004';

# COMMAND ----------

nrmRawDF_T1= spark.sql(
  "select "+
  "country, "+
  "from_unixtime(unix_timestamp(calendar_day, 'dd.MM.yyyy'), 'yyyyMMdd') time_key, "+
  "calendar_day, "+
  "distributor as distributor_code, "+
  "site as site_code, "+
  "outlet as outlet_code, "+
  "billing_document as invoice_no, "+
  "billing_type as invoice_category, "+
  "billing_item, "+
  "product as product_code, "+
  "promotion_id, "+
  "substr(promotion_id,1,2) as promotion_mechanism, "+
  "promotion_desc1 as promotion_desc, "+
  "promo_start_date, "+
  "promo_end_date, "+
  "promotion_type, "+
  "nvl(value_based_promo_disc, 0) value_based_promo_disc, "+
  "nvl(header_lvl_disc, 0) header_lvl_disc, "+
  "nvl(free_qty_in_cs, 0) free_qty_in_cs, "+
  "nvl(free_qty_in_pc, 0) free_qty_in_pc, "+
  "nvl(free_qty_val_in_cs, 0) free_qty_val_in_cs, "+
  "nvl(free_qty_val_in_pc, 0) free_qty_val_in_pc, "+
  "nvl(free_qty_retail_price_cs, 0) free_qty_retail_price_cs, "+
  "nvl(free_qty_retail_price_pc, 0) free_qty_retail_price_pc "+
  "from raw_nrm_data_dtrdis "+
  "where upper(country) ='VN' "+
  "and upper(calendar_day) != 'OVERALL RESULT' "+
  "union "+
  "select "+
  "country, "+
  "from_unixtime(unix_timestamp(calendar_day, 'dd.MM.yyyy'), 'yyyyMMdd') time_key, "+
  "calendar_day, "+
  "distributor as distributor_code, "+
  "site as site_code, "+
  "outlet as outlet_code, "+
  "billing_document as invoice_no, "+
  "billing_type as invoice_category, "+
  "billing_item, "+
  "product as product_code, "+
  "promotion_id, "+
  "substr(promotion_id,1,2) as promotion_mechanism, "+
  "promotion_desc1 as promotion_desc, "+
  "promo_start_date, "+
  "promo_end_date, "+
  "promotion_type, "+
  "nvl(value_based_promo_disc, 0) value_based_promo_disc, "+
  "nvl(header_lvl_disc, 0) header_lvl_disc, "+
  "nvl(free_qty_in_cs, 0) free_qty_in_cs, "+
  "nvl(free_qty_in_pc, 0) free_qty_in_pc, "+
  "nvl(free_qty_val_in_cs, 0) free_qty_val_in_cs, "+
  "nvl(free_qty_val_in_pc, 0) free_qty_val_in_pc, "+
  "nvl(free_qty_retail_price_cs, 0) free_qty_retail_price_cs, "+
  "nvl(free_qty_retail_price_pc, 0) free_qty_retail_price_pc "+
  "from raw_nrm_data_lindis "+
  "where upper(country) ='VN' "+
  "and upper(calendar_day) != 'OVERALL RESULT' "+
  "union "+
  "select "+
  "country, "+
  "from_unixtime(unix_timestamp(calendar_day, 'dd.MM.yyyy'), 'yyyyMMdd') time_key, "+
  "calendar_day, "+
  "distributor as distributor_code, "+
  "site as site_code, "+
  "outlet as outlet_code, "+
  "billing_document as invoice_no, "+
  "billing_type as invoice_category, "+
  "billing_item, "+
  "product as product_code, "+
  "promotion_id, "+
  "substr(promotion_id,1,2) as promotion_mechanism, "+
  "promotion_desc1 as promotion_desc, "+
  "promo_start_date, "+
  "promo_end_date, "+
  "promotion_type, "+
  "nvl(value_based_promo_disc, 0) value_based_promo_disc, "+
  "nvl(header_lvl_disc, 0) header_lvl_disc, "+
  "nvl(free_qty_in_cs, 0) free_qty_in_cs, "+
  "nvl(free_qty_in_pc, 0) free_qty_in_pc, "+
  "nvl(free_qty_val_in_cs, 0) free_qty_val_in_cs, "+
  "nvl(free_qty_val_in_pc, 0) free_qty_val_in_pc, "+
  "nvl(free_qty_retail_price_cs, 0) free_qty_retail_price_cs, "+
  "nvl(free_qty_retail_price_pc, 0) free_qty_retail_price_pc "+
  "from raw_nrm_data_basket "+
  "where upper(country) ='VN' "+
  "and upper(calendar_day) != 'OVERALL RESULT' "+
  "union "+
  "select "+
  "country, "+
  "from_unixtime(unix_timestamp(calendar_day, 'dd.MM.yyyy'), 'yyyyMMdd') time_key, "+
  "calendar_day, "+
  "distributor as distributor_code, "+
  "site as site_code, "+
  "outlet as outlet_code, "+
  "billing_document as invoice_no, "+
  "billing_type as invoice_category, "+
  "billing_item, "+
  "product as product_code, "+
  "promotion_id, "+
  "substr(promotion_id,1,2) as promotion_mechanism, "+
  "promotion_desc1 as promotion_desc, "+
  "promo_start_date, "+
  "promo_end_date, "+
  "promotion_type, "+
  "nvl(value_based_promo_disc, 0) value_based_promo_disc, "+
  "nvl(header_lvl_disc, 0) header_lvl_disc, "+
  "nvl(free_qty_in_cs, 0) free_qty_in_cs, "+
  "nvl(free_qty_in_pc, 0) free_qty_in_pc, "+
  "nvl(free_qty_val_in_cs, 0) free_qty_val_in_cs, "+
  "nvl(free_qty_val_in_pc, 0) free_qty_val_in_pc, "+
  "nvl(free_qty_retail_price_cs, 0) free_qty_retail_price_cs, "+
  "nvl(free_qty_retail_price_pc, 0) free_qty_retail_price_pc "+
  "from raw_nrm_data_voldis "+
  "where upper(country) ='VN' "+
  "and upper(calendar_day) != 'OVERALL RESULT' "+
  "union "+
  "select "+
  "country, "+
  "from_unixtime(unix_timestamp(calendar_day, 'dd.MM.yyyy'), 'yyyyMMdd') time_key, "+
  "calendar_day, "+
  "distributor as distributor_code, "+
  "site as site_code, "+
  "outlet as outlet_code, "+
  "billing_document as invoice_no, "+
  "billing_type as invoice_category, "+
  "billing_item, "+
  "product as product_code, "+
  "promotion_id, "+
  "substr(promotion_id,1,2) as promotion_mechanism, "+
  "promotion_desc1 as promotion_desc, "+
  "promo_start_date, "+
  "promo_end_date, "+
  "promotion_type, "+
  "nvl(value_based_promo_disc, 0) value_based_promo_disc, "+
  "nvl(header_lvl_disc, 0) header_lvl_disc, "+
  "nvl(free_qty_in_cs, 0) free_qty_in_cs, "+
  "nvl(free_qty_in_pc, 0) free_qty_in_pc, "+
  "nvl(free_qty_val_in_cs, 0) free_qty_val_in_cs, "+
  "nvl(free_qty_val_in_pc, 0) free_qty_val_in_pc, "+
  "nvl(free_qty_retail_price_cs, 0) free_qty_retail_price_cs, "+
  "nvl(free_qty_retail_price_pc, 0) free_qty_retail_price_pc "+
  "from raw_nrm_data_oth "+
  "where upper(country) ='VN' "+
  "and upper(calendar_day) != 'OVERALL RESULT'"
).repartition(500)
nrmRawDF_T1.createOrReplaceTempView("nrm_raw_df_t1")

nrmRawDFDT = spark.sql("select nrm.* "+
                       "from nrm_raw_df_t1 nrm "+
                       "inner join list_site dt "+
                       "on nrm.site_code = dt.site_code")
nrmRawDFDT= nrmRawDFDT.repartition(20)
nrmRawDFDT.createOrReplaceTempView ("nrm_raw_data")

# COMMAND ----------

# MAGIC %md ##### run data load validations

# COMMAND ----------

# MAGIC %run ./NRMDSS_LoadValidation

# COMMAND ----------

# MAGIC %md ##### run summary validations

# COMMAND ----------

# MAGIC %run ./NRMDSS_SummaryValidation
