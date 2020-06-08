# Databricks notebook source
# MAGIC %md ### Deploy latest mlFlow registry Model to Databricks batch

# COMMAND ----------

import os
import requests

def download_file(data_uri, data_path):
  if os.path.exists(data_path):
      print("File {} already exists".format(data_path))
  else:
      print("Downloading {} to {}".format(data_uri,data_path))
      rsp = requests.get(data_uri)
      with open(data_path, 'w') as f:
          f.write(requests.get(data_uri).text)

data_path = "/dbfs/tmp/mlflow-wine-quality.csv"
data_uri = "https://raw.githubusercontent.com/mlflow/mlflow/master/examples/sklearn_elasticnet_wine/wine-quality.csv"
download_file(data_uri, data_path)
wine_data_path = "/tmp/mlflow-wine-quality.csv"




# COMMAND ----------

# MAGIC %md ###Get Name of Model

# COMMAND ----------

dbutils.widgets.text(name="model_name", defaultValue="automation-wine-model", label="Model Name")
dbutils.widgets.text(name="stage", defaultValue="staging", label="Stage")
dbutils.widgets.text(name="phase", defaultValue="qa", label="Phase")

# COMMAND ----------

model_name = dbutils.widgets.get("model_name")
stage = dbutils.widgets.get("stage")
phase = dbutils.widgets.get("phase")


# COMMAND ----------

# MAGIC %md ### Get the latest version of the model that was put into Staging

# COMMAND ----------

import mlflow
import mlflow.sklearn

client = mlflow.tracking.MlflowClient()
latest_model = client.get_latest_versions(name=model_name, stages=[stage])
# print(latest_model[0])

# COMMAND ----------
from mlflow import pyfunc


model_uri = "runs:/{}/model".format(latest_model[0].run_id)
udf = pyfunc.spark_udf(spark, model_uri)

# COMMAND ----------

# MAGIC %sh wget https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv

# COMMAND ----------

data_spark = spark.read.csv(wine_data_path, header=True)
predictions = data_spark.select(udf(*sample_test_data.columns).alias('prediction'), "*")


# COMMAND ----------

dbutils.notebook.exit(True)
