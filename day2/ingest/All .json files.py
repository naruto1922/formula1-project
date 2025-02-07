# Databricks notebook source
# MAGIC %md
# MAGIC #Ingesting Constructors, Drivers, Results, Pitstops.json files 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@databrickscoursestgacc.dfs.core.windows.net/"))

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING , name STRING ,nationality STRING, url STRING"
name_schema=StructType(fields=[StructField("forename", StringType()), StructField("surname", StringType()), ])
drivers_schema= StructType(fields=[StructField("driverId", IntegerType(), False), 
                                   StructField("driverRef", StringType(),True), 
                                   StructField("number", IntegerType(),True), 
                                   StructField("code", StringType(),True), 
                                   StructField("name", name_schema), 
                                   StructField("dob", DateType(),True), 
                                   StructField("nationality", StringType(),True),
                                   StructField("url", StringType(),True)])

results_schema=StructType(fields=[StructField("resultId", IntegerType(), False),
                                  StructField("raceId", IntegerType(), True),
                                  StructField("driverId", IntegerType(), True),
                                  StructField("constructorId", IntegerType(), True),
                                  StructField("number", IntegerType(), True),
                                  StructField("grid", IntegerType(), True),
                                  StructField("position", IntegerType(), True),
                                  StructField("positionText", StringType(), True),
                                  StructField("positionOrder", IntegerType(), True),
                                  StructField("points", FloatType(), True),
                                  StructField("laps", IntegerType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("milliseconds", IntegerType(), True),
                                  StructField("fastestLap", IntegerType(), True),
                                  StructField("rank", IntegerType(), True),
                                  StructField("fastestLapTime", StringType(), True),
                                  StructField("fastestLapSpeed", FloatType(), True),
                                  StructField("statusId", StringType(), True)])

pit_stops_schema= StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("stop", StringType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)])


# COMMAND ----------

constructors_df = spark.read.format("json").schema(constructors_schema).load("abfss://raw@databrickscoursestgacc.dfs.core.windows.net/constructors.json")
# display(constructors_df)
drivers_df = spark.read.format("json").schema(drivers_schema).load("abfss://raw@databrickscoursestgacc.dfs.core.windows.net/drivers.json")
# display(drivers_df)
results_df = spark.read.format("json").schema(results_schema).load("abfss://raw@databrickscoursestgacc.dfs.core.windows.net/results.json")
# display(results_df)
pit_stops_df= spark.read.format("json").option("multiline", "true").schema(pit_stops_schema).load("abfss://raw@databrickscoursestgacc.dfs.core.windows.net/pit_stops.json")
# display(pit_stops_df)

# COMMAND ----------

#transforming constructors_df
constructors_final_df= constructors_df.withColumnRenamed("constructorId", "constructor_id") \
                                        .withColumnRenamed("constructorRef", "constructor_ref") \
                                            .withColumn("ingestion_date", current_timestamp()) \
                                                .drop("url")
# display(constructors_final_df)
#transforming drivers_df
drivers_final_df=drivers_df.withColumnRenamed("driverId", "driver_id") \
                            .withColumnRenamed("driverRef", "driver_ref") \
                                .withColumn("name", concat(col("name.forename") ,lit(" "), col("name.surname"))) \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                        .drop("url")

# display(drivers_final_df)
results_final_df=results_df.withColumnRenamed("resultId", "result_id") \
                            .withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                        .withColumnRenamed("positiontext", "position_text") \
                                            .withColumnRenamed("positionOrder", "position_order") \
                                                .withColumnRenamed("fastestlap", "fastest_lap") \
                                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                                        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                                            .withColumn("ingestion_date", current_timestamp()) \
                                                                .drop("statusId")
# display(results_final_df)
pit_stops_final_df=pit_stops_df.withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                    .withColumn("ingestion_date", current_timestamp())
# display(pit_stops_final_df)                                    

# COMMAND ----------

#writing these files in the processed container.
# constructors_final_df.write.mode("overwrite").format("parquet").save("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/constructors")
# # display(spark.read.format("parquet").load("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/constructors"))
# drivers_final_df.write.mode("overwrite").format("parquet").save("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/drivers")
# display(spark.read.format("parquet").load("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/drivers"))
# results_final_df.write.mode("overwrite").partitionBy("race_id").format("parquet").save("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/results")
# display(spark.read.format("parquet").load("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/results"))
pit_stops_final_df.write.mode("overwrite").format("parquet").save("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/pit_stops")
# display(spark.read.format("parquet").load("abfss://processed@databrickscoursestgacc.dfs.core.windows.net/pit_stops"))

# COMMAND ----------

