# Databricks notebook source
dbutils.widgets.text("RunType", "once", "Set once to run as a batch")
dbutils.widgets.text("ProcessingTime", "5 seconds", "Set the microbatch interval")

# COMMAND ----------


once = True if dbutils.widgets.get("RunType")=="once" else False
processing_time = dbutils.widgets.get("ProcessingTime")
if once:
    print(f"Starting sbit in batch mode.")
else:
    print(f"Starting sbit in stream mode with {processing_time} microbatch.")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", True)
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")


# COMMAND ----------

# MAGIC
# MAGIC %run ./02-setup

# COMMAND ----------

# MAGIC %run ./03-history-loader

# COMMAND ----------

SH = SetupHelper()
HL = HistoryLoader()

# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

setup_required = spark.sql(f"SHOW DATABASES IN hive_metastore").filter(f"databaseName == '{SH.db_name}'").count() != 1
if setup_required:
    SH.setup()
    SH.validate()
    HL.load_history()
    HL.validate()
else:
    spark.sql(f"USE hive_metastore.{SH.db_name}")

# COMMAND ----------

# MAGIC %run ./04-bronze

# COMMAND ----------

# MAGIC %run ./05-silver

# COMMAND ----------

# MAGIC %run ./06-gold

# COMMAND ----------

BZ = Bronze()
SL = Silver()
GL = Gold()

# COMMAND ----------

BZ.consume(once, processing_time)

# COMMAND ----------

SL.upsert(once, processing_time)

# COMMAND ----------

GL.upsert(once, processing_time)
