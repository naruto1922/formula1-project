# Databricks notebook source
# MAGIC %run ../includes/configurations 

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

circuits_schema= StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", StringType(), True),
                                    StructField("lng", StringType(), True),
                                    StructField("alt", StringType(), True),
                                    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df= spark.read.csv(f"{raw_folder_path}circuits.csv", header=True,schema=circuits_schema)
display(circuits_df)

# COMMAND ----------

circuits_selected_df= circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))
display(circuits_selected_df)

# COMMAND ----------

circuits_renamed_df= circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude")

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

circuits_final_df=circuits_renamed_df.withColumn("ingestion_time",current_timestamp())

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS processed
# MAGIC MANAGED LOCATION "abfss://processed@databrickscoursestgacc.dfs.core.windows.net/";

# COMMAND ----------

