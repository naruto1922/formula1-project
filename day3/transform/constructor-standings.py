# Databricks notebook source
# MAGIC %run ../includes/configurations

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

race_results_df=spark.read.format("parquet").load(f"{presentation_folder_path}/race_results")
display(race_results_df)

# COMMAND ----------

constructor_standings_df= race_results_df.groupby("race_year", "team").agg(sum("points").alias("total_points"), count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

constructor_rank_spec=Window.partitionBy("race_year").orderBy(col("total_points").desc(), col("wins").desc())
final_df=constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year=2020"))

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").saveAsTable("presentation.constructor_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM presentation.constructor_standings

# COMMAND ----------

