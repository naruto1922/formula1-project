# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

qualifying_schema=StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.format("json").options(multiline="true").schema(qualifying_schema).load("abfss://raw@databrickscoursestgacc.dfs.core.windows.net/qualifying")
display(qualifying_df)

# COMMAND ----------

qualifying_final_df=qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
    .withColumnRenamed("raceId","race_id") \
        .withColumnRenamed("driverId","driver_id") \
            .withColumnRenamed("constructorId","constructor_id") \
                .withColumn("ingestion_date", current_timestamp())
display(qualifying_final_df)

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("delta").saveAsTable("processed.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM processed.qualifying