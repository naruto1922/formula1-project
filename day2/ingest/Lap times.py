# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@databrickscoursestgacc.dfs.core.windows.net/lap_times"))

# COMMAND ----------

lap_times_schema=StructType(fields=[
    StructField("race_id", IntegerType(), False),
    StructField("driver_id", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df=spark.read.format("csv").schema(lap_times_schema).load("abfss://raw@databrickscoursestgacc.dfs.core.windows.net/lap_times")

# COMMAND ----------

lap_times_final_df=lap_times_df.withColumn("ingestion_date", current_timestamp())
display(lap_times_final_df)

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").format("parquet").save("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/lap_times")
display(spark.read.format("parquet").load("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/lap_times"))

# COMMAND ----------

