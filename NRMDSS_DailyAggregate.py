# Databricks notebook source
# MAGIC %md ##### Calling ON Invoice processing

# COMMAND ----------

# MAGIC %run ./NRMDSS_OnInvoiceProcessing

# COMMAND ----------

# MAGIC %md ##### Calling OFF Invoice processing

# COMMAND ----------

# MAGIC %run ./NRMDSS_OffInvoiceProcessing

# COMMAND ----------

# MAGIC %md ##### Calling Non UL Gift processing

# COMMAND ----------

# MAGIC %run ./NRMDSS_NonULGiftProcessing

# COMMAND ----------

year_id= int(dbutils.widgets.get("ProcessingYear"))
month_id= int(str(year_id) + dbutils.widgets.get("ProcessingMonth"))

# COMMAND ----------

nrmOnInvoiceData.createOrReplaceTempView("nrm_on_invoice_data")
nrmOffInvoiceData.createOrReplaceTempView("nrm_off_invoice_data")
nonULGiftData.createOrReplaceTempView("nrm_non_ul_gift_data")

# COMMAND ----------

final_on_invoice = spark.sql("""
                              select 
                                invoice_no,
                                product_code,
                                standard_product_group_code,
                                promotion_id,
                                IO,
                                promotion_mechanism,
                                promotion_desc,
                                promotion_type,
                                budget_holder_group,
                                invoice_type,
                                promo_start_date,
                                promo_end_date,
                                nvl(sum(value_based_promo_disc),0) value_based_promo_disc,
                                nvl(sum(header_lvl_disc),0) header_lvl_disc,
                                nvl(sum(free_quantity_value),0) free_quantity_value,
                                nvl(sum(promotion_value),0) promotion_value,
                                distributor_code,
                                site_code,
                                outlet_code,
                                banded,
                                'NA' gift_price_available,
                                country,
                                year_id,
                                month_id,
                                time_key
                              from nrm_on_invoice_data
                              group by invoice_no,product_code,standard_product_group_code,promotion_id,IO,promotion_mechanism,promotion_desc,
                              promotion_type,budget_holder_group,invoice_type,promo_start_date,promo_end_date,distributor_code,site_code,
                              outlet_code,banded,country,year_id,month_id,time_key
                            """)
#8610055
#9785398

# COMMAND ----------

final_off_invoice = spark.sql("""
                              select 
                                invoice_no,
                                standard_product_group_code as product_code,
                                standard_product_group_code,
                                promotion_id,
                                IO,
                                promotion_mechanism,
                                promotion_desc,
                                promotion_type,
                                budget_holder_group,
                                invoice_type,
                                promo_start_date,
                                promo_end_date,
                                nvl(sum(value_based_promo_disc),0) value_based_promo_disc,
                                nvl(sum(header_lvl_disc),0) header_lvl_disc,
                                nvl(sum(free_quantity_value),0) free_quantity_value,
                                nvl(sum(promotion_value),0) promotion_value,
                                distributor_code,
                                site_code,
                                outlet_code,
                                banded,
                                'NA' gift_price_available,
                                country,
                                year_id,
                                month_id,
                                time_key
                              from nrm_off_invoice_data
                              group by invoice_no,standard_product_group_code,promotion_id,IO,promotion_mechanism,promotion_desc,
                              promotion_type,budget_holder_group,invoice_type,promo_start_date,promo_end_date,distributor_code,site_code,
                              outlet_code,banded,country,year_id,month_id,time_key
                              """)

# COMMAND ----------

final_non_ul_gift_data = spark.sql("""
                                    select 
                                      invoice_no,
                                      standard_product_group_code as product_code,
                                      standard_product_group_code,
                                      promotion_id,
                                      IO,
                                      promotion_mechanism,
                                      promotion_desc,
                                      promotion_type,
                                      budget_holder_group,
                                      invoice_type,
                                      promo_start_date,
                                      promo_end_date,
                                      nvl(sum(value_based_promo_disc),0) value_based_promo_disc,
                                      nvl(sum(header_lvl_disc),0) header_lvl_disc,
                                      nvl(sum(free_quantity_value),0) free_quantity_value,
                                      nvl(sum(promotion_value),0) promotion_value,
                                      distributor_code,
                                      site_code,
                                      outlet_code,
                                      banded,
                                      gift_price_available,
                                      country,
                                      year_id,
                                      month_id,
                                      time_key
                                    from nrm_non_ul_gift_data
                                    group by invoice_no,standard_product_group_code,promotion_id,IO,promotion_mechanism,promotion_desc,
                                    promotion_type,budget_holder_group,invoice_type,promo_start_date,promo_end_date,distributor_code,site_code,
                                    outlet_code,banded,gift_price_available,country,year_id,month_id,time_key
                                    """)

# COMMAND ----------

final_non_processed_mec = spark.sql ("""
                                     select 
                                        invoice_no,
                                        product_code,
                                        'NA' standard_product_group_code,
                                        promotion_id,
                                        IO,
                                        promotion_mechanism,
                                        promotion_desc,
                                        promotion_type,
                                        'NA' budget_holder_group,
                                        'OFF' invoice_type,
                                        promo_start_date,
                                        promo_end_date,
                                        nvl(sum(value_based_promo_disc),0) value_based_promo_disc,
                                        nvl(sum(header_lvl_disc),0) header_lvl_disc,
                                        nvl(sum(free_quantity_value),0) free_quantity_value,
                                        nvl(sum(promotion_value),0) promotion_value,
                                        distributor_code,
                                        site_code,
                                        outlet_code,
                                        'Non-Banded' banded,
                                        'NA' gift_price_available,
                                        country,
                                        year_id,
                                        month_id,
                                        time_key
                                      from edge.processed_nrm_data
                                      where year_id={} and month_id={}
                                      and (on_off_flag=0 and promotion_mechanism != 'FQ')
                                      group by invoice_no,product_code, promotion_id,IO,promotion_mechanism,promotion_desc,
                                      promotion_type,promo_start_date,promo_end_date,distributor_code,site_code,
                                      outlet_code,country,year_id,month_id,time_key
                                     """.format(year_id, month_id))

# COMMAND ----------

transformed_nrm_data = final_on_invoice.unionAll(final_off_invoice) \
                                       .unionAll(final_non_ul_gift_data) \
                                       .unionAll(final_non_processed_mec)
