# Databricks notebook source
from dbxdemo import appendcol

# COMMAND ----------

display(appendcol.with_status(spark.read.parquet("/databricks-datasets/samples/lending_club/parquet/")))


