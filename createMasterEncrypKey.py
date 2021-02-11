# Databricks notebook source
"""
Author       : Saikat Sengupta
Created Date : 06/12/2020
Modified Date: 06/12/2020
Purpose      : Generate MASTER KEY for encryption. Later we share this with Landscape Team
"""

# COMMAND ----------

from cryptography.fernet import Fernet

MASTER_KEY = Fernet.generate_key()

with open('/dbfs/mnt/adls/EDGE_Analytics/TestEncryption/masterKeyFile.asc', 'wb') as f:
    f.write(MASTER_KEY)
