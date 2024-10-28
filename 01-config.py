# Databricks notebook source
class Config():    
    def __init__(self):      
        self.base_dir_data = "/mnt/spark/data"
        self.base_dir_checkpoint = "/mnt/spark/checkpoint"
        self.db_name = "sbit_db"
        self.maxFilesPerTrigger = 10


# COMMAND ----------

# # dbutils.fs.mkdirs("/mnt/spark/data/raw")
# # dbutils.fs.mkdirs("/mnt/spark/data/test_data")
# # dbutils.fs.mkdirs("/mnt/spark/checkpoint")
# dbutils.fs.mkdirs("/mnt/spark/data/raw/registered_users_bz")
# dbutils.fs.mkdirs("/mnt/spark/data/raw/gym_logins_bz")
# dbutils.fs.mkdirs("/mnt/spark/data/raw/kafka_multiplex_bz")
