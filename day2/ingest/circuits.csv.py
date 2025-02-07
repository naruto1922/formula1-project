# Databricks notebook source
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

circuits_df= spark.read.csv("abfss://raw@databrickscoursestgacc.dfs.core.windows.net/circuits.csv", header=True,schema=circuits_schema)
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

dbutils.fs.ls("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").save("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/circuits")

# COMMAND ----------

display(spark.read.format("parquet").load("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/circuits"))

# COMMAND ----------

