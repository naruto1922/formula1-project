# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@databrickscoursestgacc.dfs.core.windows.net/"))

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                StructField("year",IntegerType(),True),
                                StructField("round",IntegerType(),True),
                                StructField("circuitId",IntegerType(),True),
                                StructField("name",StringType(),True),
                                StructField("date",DateType(),True),
                                StructField("time",StringType(),True),
                                StructField("url",StringType(),True)])

# COMMAND ----------

races_df=spark.read.csv("abfss://raw@databrickscoursestgacc.dfs.core.windows.net/races.csv",header=True,schema=races_schema)
display(races_df)

# COMMAND ----------

races_renamed_df=races_df.withColumnRenamed("circuitId","circuit_id") \
                          .withColumnRenamed("year","race_year") \
                          .withColumnRenamed("raceId","race_id") 
display(races_renamed_df)

# COMMAND ----------

races_transformed_df=races_renamed_df.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),col("time")),"yyyy-MM-dd HH:mm:ss")).withColumn("ingestion_date",current_timestamp())
display(races_transformed_df)

# COMMAND ----------

races_selected_df=races_transformed_df.select("race_id","race_year","round","circuit_id","name","ingestion_date","race_timestamp")
display(races_selected_df)

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("parquet").save("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/races")

# COMMAND ----------

display(spark.read.parquet("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/races"))

# COMMAND ----------

