# Azure Databricks Course - Day 2

## Overview
This document summarizes the key learnings from Day 2 of the Azure Databricks course, focusing on data ingestion and transformation.

## Key Learnings

### 1. Ingesting .CSV Files from One Location to Another
- Explored various Spark functions for handling CSV files:
  - **spark.sql.functions** for data manipulation
  - **spark.sql.types** for defining data types
  - **Specifying schema** using `StructType`
  - **Renaming columns** using `withColumnRenamed`
  - **Selecting specific columns** with `df.select(col("..."))`
  - **Transforming columns** using `withColumn("NewColumnName", function)`
  - **Partitioning** while writing a file for optimized storage and retrieval