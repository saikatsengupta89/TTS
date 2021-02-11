# Databricks notebook source
"""
Author       : Saikat Sengupta
Created Date : 05/12/2020
Modified Date: 06/12/2020
Purpose      : Register Encrypt and Decrypt UDFs before encryption/decryption
"""

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet

# Define Encrypt User Defined Function 
def encrypt_val(org_text,MASTER_KEY):
  f = Fernet(MASTER_KEY)
  if (not org_text or len(org_text)==0):
    return None
  else:
    clear_text_b=bytes(org_text, 'utf-8')
    cipher_text = f.encrypt(clear_text_b)
    cipher_text = str(cipher_text.decode('ascii'))
    return cipher_text
  
# Define decrypt user defined function 
def decrypt_val(cipher_text,MASTER_KEY):
  if (not cipher_text or len(cipher_text)==0):
    return None
  else:
    f = Fernet(MASTER_KEY)
    clear_val=f.decrypt(cipher_text.encode()).decode()
    return clear_val
    

# Register UDFs for encryption and decryption and using with dataframe operations
encrypt_pii = udf(encrypt_val, StringType())
decrypt_pii = udf(decrypt_val, StringType())
    
# Register UDFs for encryption and decryption and using with spark SQL operations
encrypt = lambda x,y: encrypt_val(x,y)
decrypt = lambda x,y: decrypt_val(x,y)
spark.udf.register("encrypt_pii", encrypt)
spark.udf.register("decrypt_pii", decrypt)
