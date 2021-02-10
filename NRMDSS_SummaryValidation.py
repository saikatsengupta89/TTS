# Databricks notebook source
from datetime import datetime
dateTimeObj = datetime.now()
dateTime= str(dateTimeObj.strftime("%Y%m%d%H%M%S"))

# COMMAND ----------

pathExceptionValidation=f"/mnt/adls/TTS/processed/transactions/monthly/nrm_dss_quality_chck/{yearMonth}/summary_validation"

# COMMAND ----------

nrmValFilePath= f"/mnt/adls/TTS/raw/daily/nrm_dss_val/{yearMonth}/"

fileInfo = dbutils.fs.ls (nrmValFilePath)
fileList= [str(i.name) for i in fileInfo]
fileListFiltered= list(filter(lambda x: x.__contains__("csv"), fileList))

totalRows=0
npArrayList = []
for fileName in fileListFiltered:
  #if fileName=="LE VN NRM DSS G1 20190101 V18.csv":
    #continue
  path= '/dbfs'+nrmValFilePath+f"{fileName}"
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
nrmRawVal=pd.DataFrame(combNpArray)

# COMMAND ----------

colNames=["calendar_day", "site_code", "distributor_type", "value_based_promo_disc", 
          "header_lvl_disc", "free_qty_in_cs", "free_qty_in_pc"]
nrmRawVal.columns = colNames

nrmRawVal['value_based_promo_disc']=  nrmRawVal['value_based_promo_disc'].astype(str).str.replace(',','').str.replace('-','').astype(float)
nrmRawVal['header_lvl_disc']       =  nrmRawVal['header_lvl_disc'].astype(str).str.replace(',','').str.replace('-','').astype(float)
nrmRawVal['free_qty_in_cs']        =  nrmRawVal['free_qty_in_cs'].astype(str).str.replace(',','').str.replace('-','').astype(float)
nrmRawVal['free_qty_in_pc']        =  nrmRawVal['free_qty_in_pc'].astype(str).str.replace(',','').str.replace('-','').astype(float)

# COMMAND ----------

nrmDssValSchema= types.StructType([
  types.StructField("calendar_day", types.StringType()),
  types.StructField("site_code", types.StringType()),
  types.StructField("distributor_type", types.StringType()),
  types.StructField("value_based_promo_disc", types.FloatType()),
  types.StructField("header_lvl_disc", types.FloatType()),
  types.StructField("free_qty_in_cs", types.FloatType()),
  types.StructField("free_qty_in_pc", types.FloatType())
])

nrmDSSValDF= spark.createDataFrame(nrmRawVal, nrmDssValSchema)
nrmDSSValDF.createOrReplaceTempView("nrm_dss_val_data")

nrmDSSValDF_T1= spark.sql("select "+
                          "from_unixtime(unix_timestamp(calendar_day, 'dd.MM.yyyy'), 'yyyyMMdd') time_key, "+
                          "site_code, "+
                          "value_based_promo_disc, "+
                          "header_lvl_disc, "+
                          "free_qty_in_cs, "+
                          "free_qty_in_pc "+
                          "from nrm_dss_val_data "+
                          "where upper(distributor_type) like 'DT%'"
                         )
nrmDSSValDF_T1.createOrReplaceTempView("nrm_dss_val_data_t1")

# COMMAND ----------

nrmValExceptions= spark.sql (
"""
select 
  site_code,
  act_VBPD,
  tgt_VBPD,
  delta_VBPD,
  case when tgt_VBPD=0 then 0 else round(abs(delta_VBPD)/tgt_VBPD *100,4) end delta_VBPD_prcntg,
  act_HLD,
  tgt_HLD,
  delta_HLD,
  case when tgt_HLD=0 then 0 else round(abs(delta_HLD)/tgt_HLD *100,4) end delta_HLD_prcntg,
  act_QTY_CS,
  tgt_QTY_CS,
  delta_QTY_CS,
  case when tgt_HLD=0 then 0 else round(abs(delta_QTY_CS)/tgt_QTY_CS *100,4) end delta_QTY_CS_prcntg,
  act_QTY_PC,
  tgt_QTY_PC,
  delta_QTY_PC,
  case when tgt_HLD=0 then 0 else round(abs(delta_QTY_PC)/tgt_QTY_PC *100,4) end delta_QTY_PC_prcntg
from  
(select  
  q1.site_code, 
  nvl(q1.value_based_promo_disc,0) act_VBPD, 
  nvl(q2.value_based_promo_disc,0) tgt_VBPD, 
  (nvl(q1.value_based_promo_disc,0) - nvl(q2.value_based_promo_disc,0)) delta_VBPD, 
  nvl(q1.header_lvl_disc,0) act_HLD, 
  nvl(q2.header_lvl_disc,0) tgt_HLD, 
  (nvl(q1.header_lvl_disc,0) - nvl(q2.header_lvl_disc,0)) delta_HLD, 
  nvl(q1.free_qty_in_cs,0) act_QTY_CS, 
  nvl(q2.free_qty_in_cs,0) tgt_QTY_CS, 
  (nvl(q1.free_qty_in_cs,0) - nvl(q2.free_qty_in_cs,0)) delta_QTY_CS, 
  nvl(q1.free_qty_in_pc,0) act_QTY_PC, 
  nvl(q2.free_qty_in_pc,0) tgt_QTY_PC, 
  (nvl(q1.free_qty_in_pc,0) - nvl(q2.free_qty_in_pc,0)) delta_QTY_PC 
  from 
  (select 
    site_code, 
    abs(sum(value_based_promo_disc)) value_based_promo_disc, 
    abs(sum(header_lvl_disc)) header_lvl_disc, 
    abs(sum(free_qty_in_cs)) free_qty_in_cs, 
    abs(sum(free_qty_in_pc)) free_qty_in_pc 
  from nrm_raw_data 
  group by site_code, time_key 
  ) q1 
  left outer join  
  (select 
      site_code, 
      abs(sum(value_based_promo_disc)) value_based_promo_disc, 
      abs(sum(header_lvl_disc)) header_lvl_disc, 
      abs(sum(free_qty_in_cs)) free_qty_in_cs, 
      abs(sum(free_qty_in_pc)) free_qty_in_pc 
    from nrm_dss_val_data_t1 
    group by site_code, time_key 
  ) q2  
  on q1.site_code = q2.site_code 
)
where (abs(delta_VBPD) + abs(delta_HLD) + abs(delta_QTY_CS) + abs(delta_QTY_PC)) >= 10
"""
)
display(nrmValExceptions)

# write the exception in a file in raw layer
if (nrmValExceptions.count() > 0):
  nrmValExceptions.repartition(1) \
                .write \
                .mode("append") \
                .option("header","true") \
                .csv(pathExceptionValidation)

# COMMAND ----------

nrmValExceptions.createOrReplaceTempView("data_exceptions")
breachedThresholdDF = spark.sql("""
                                 select * from data_exceptions
                                 where ( delta_VBPD_prcntg >= {} or 
                                         delta_HLD_prcntg >= {} or
                                         delta_QTY_CS_prcntg >= {} or
                                         delta_QTY_PC_prcntg >= {}
                                       )
                                """.format(validationThreshold, validationThreshold, validationThreshold, validationThreshold)
                               )

# CLEAN ALL TEMPORARY PANDAS IN-MEM DATAFRAME BEFORE RAISING EXCEPTION
if(breachedThresholdDF.count() > 0):
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
  raise Exception ("Exception: Summary Vaidation failed")
else:
  print("Delta values within threshold. No outliers.")
# except Exception as e:
#   raise dbutils.notebook.exit(e)
