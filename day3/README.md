# Azure Databricks Course - Day 3

## Overview
This document summarizes the key learnings from Day 3 of the Azure Databricks course, focusing on advanced data transformation using joins and window functions.

## Key Learnings

### 1. Joins and Window functions in pyspark
- Explored the different types of joins and window functions
    - Used joins to get race-results containing only the necessary data from all the tables.
    - Used window functions to rank the drivers and teams based on the points scored.

### 2. Saving as tables
- Converted the ingested parquet data to managed delta tables using df.write.mode("OverWrite").format("delta").saveAsTable
- Created database in databricks catalog for processed and presentation data.
- Modified notebooks to save the data as table in the database itself.cl

### Note: Run the python files in ingest folder in your own spark cluster hosted in your Azure Databricks workspace.
  