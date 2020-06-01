# Databricks notebook source
# MAGIC %md ### Training a model and adding to the mlFlow registry

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text(name = "model_name", defaultValue = "automation-wine-model", label = "Model Name")
# dbutils.widgets.combobox(name = "trigger_pipeline", defaultValue = "False", choices=["True","False"],label = "Trigger Pipeline")

# COMMAND ----------

model_name=dbutils.widgets.get("model_name")

# COMMAND ----------

# MAGIC %md ### Connect to an MLflow tracking server
# MAGIC 
# MAGIC MLflow can collect data about a model training session, such as validation accuracy. It can also save artifacts produced during the training session, such as a PySpark pipeline model.
# MAGIC 
# MAGIC By default, these data and artifacts are stored on the cluster's local filesystem. However, they can also be stored remotely using an [MLflow Tracking Server](https://mlflow.org/docs/latest/tracking.html).

# COMMAND ----------

import mlflow
mlflow.__version__

# Using the hosted mlflow tracking server

# COMMAND ----------

# MAGIC %md ## Training a model

# COMMAND ----------

# MAGIC %md ### Download training data 
# MAGIC 
# MAGIC First, download the [wine qualities dataset (published by Cortez et al.)](https://archive.ics.uci.edu/ml/datasets/wine+quality) that will be used to train the model.

# COMMAND ----------

# MAGIC %sh wget https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv --no-check-certificate

# COMMAND ----------

import os
import requests
from pyspark.sql.functions import col

def get_username():
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("user").get()
def get_db():
    return get_username().replace("@", "_").replace(".", "_")
def get_home():
    return "/Users/{}".format(get_username())

def download_file(data_uri, data_path):
    if os.path.exists(data_path):
        print("File {} already exists".format(data_path))
    else:
        print("Downloading {} to {}".format(data_uri,data_path))
        rsp = requests.get(data_uri)
        with open(data_path, 'w') as f:
            f.write(requests.get(data_uri).text)

def download_wine_file(home, data_path):
    download_file(data_uri, data_path)
    final_path = f"{home}/automation/mlflow/wine-quality/wine-quality.csv"
    print(f"Copying file to {final_path}")
    dbutils.fs.cp(f"/tmp/mlflow-wine-quality.csv", final_path)
    return final_path
      
username = get_username()
home = get_home()
db = get_db()

# data_path = "/dbfs/tmp/mlflow-wine-quality.csv"
temp_data_path = f"/dbfs/tmp/{username}/mlflow-wine-quality.csv"
data_uri = "https://raw.githubusercontent.com/mlflow/mlflow/master/examples/sklearn_elasticnet_wine/wine-quality.csv"
dbfs_wine_data_path = download_wine_file(home, temp_data_path)
wine_data_path = f"/dbfs/{dbfs_wine_data_path}"


# wine_df = spark.read.format("csv").option("header", "true").load(final_path).cache()
# wine_df = wine_df.select(*(col(column).cast("float").alias(column.replace(" ", "_"))  for column in wine_df.columns))
# wine_df = wine_df.withColumn("quality", col("quality").cast("integer"))

# COMMAND ----------

# MAGIC %md ### In an MLflow run, train and save an ElasticNet model for rating wines
# MAGIC 
# MAGIC We will train a model using Scikit-learn's Elastic Net regression module. We will fit the model inside a new MLflow run (training session), allowing us to save performance metrics, hyperparameter data, and model artifacts for future reference. If MLflow has been connected to a tracking server, this data will be persisted to the tracking server's file and artifact stores, allowing other users to view and download it. For more information about model tracking in MLflow, see the [MLflow tracking reference](https://www.mlflow.org/docs/latest/tracking.html).
# MAGIC 
# MAGIC Later, we will use the saved MLflow model artifacts to deploy the trained model to Azure ML for real-time serving.

# COMMAND ----------

import os
import warnings
import sys

import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet

import mlflow
import mlflow.sklearn


def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2


def train_model(wine_data_path, model_path, alpha, l1_ratio):
    warnings.filterwarnings("ignore")
    np.random.seed(40)

    # Read the wine-quality csv file (make sure you're running this from the root of MLflow!)
    data = pd.read_csv(wine_data_path, sep=None)

    # Split the data into training and test sets. (0.75, 0.25) split.
    train, test = train_test_split(data)

    # The predicted column is "quality" which is a scalar from [3, 9]
    train_x = train.drop(["quality"], axis=1)
    test_x = test.drop(["quality"], axis=1)
    train_y = train[["quality"]]
    test_y = test[["quality"]]

    # Start a new MLflow training run 
    with mlflow.start_run():
        # Fit the Scikit-learn ElasticNet model
        lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
        lr.fit(train_x, train_y)

        predicted_qualities = lr.predict(test_x)

        # Evaluate the performance of the model using several accuracy metrics
        (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

        print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
        print("  RMSE: %s" % rmse)
        print("  MAE: %s" % mae)
        print("  R2: %s" % r2)

        # Log model hyperparameters and performance metrics to the MLflow tracking server
        # (or to disk if no)
        mlflow.log_param("alpha", alpha)
        mlflow.log_param("l1_ratio", l1_ratio)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("mae", mae)

        mlflow.sklearn.log_model(lr, model_path)
        
        return mlflow.active_run().info.run_uuid

# COMMAND ----------

alpha_1 = 0.75
l1_ratio_1 = 0.25
model_path = 'model'
run_id1 = train_model(wine_data_path=wine_data_path, model_path=model_path, alpha=alpha_1, l1_ratio=l1_ratio_1)
model_uri = "runs:/"+run_id1+"/model"

# COMMAND ----------

print(model_uri)

# COMMAND ----------

# MAGIC %md ## Register the Model in the Model Registry

# COMMAND ----------

import time
result = mlflow.register_model(
    model_uri,
    model_name
)
time.sleep(10)
version = result.version

# COMMAND ----------

# MAGIC %md ### Transitioning the model to 'Staging"

# COMMAND ----------

import mlflow
client = mlflow.tracking.MlflowClient()

client.transition_model_version_stage(
    name=model_name,
    version=version,
    stage="staging")

# COMMAND ----------

# MAGIC %md #Trigger Azure Dev Ops ML Governance Pipeline

# COMMAND ----------

# trigger=dbutils.widgets.get("trigger_pipeline")
# if trigger == "True":
#   from azure.devops.connection import Connection
#   from msrest.authentication import BasicAuthentication
#   from azure.devops.v6_0.pipelines.models import RunPipelineParameters,Variable

#   # Fill in with your personal access token and org URL
#   personal_access_token = dbutils.secrets.get('ml-gov','ado-token')
#   organization_url = 'https://dev.azure.com/ML-Governance'

#   # Create a connection to the org
#   credentials = BasicAuthentication('', personal_access_token)
#   connection = Connection(base_url=organization_url, creds=credentials)

#   # Get a client (the "core" client provides access to projects, teams, etc)
#   pipeline_client=connection.clients_v6_0.get_pipelines_client()

#   #Set the variables for the pipeline
#   variable=Variable(value=model_name)
#   variables={'model_name':variable}
#   run_parameters=RunPipelineParameters(variables=variables)
#   print(run_parameters)

#   # Run pipeline in MKL Goverance Project V2 with id 6 (ML Goverance V3))
#   runPipeline = pipeline_client.run_pipeline(run_parameters=run_parameters,project='ML Governance V2',pipeline_id=6)
#   print('Pipeline is triggered. Please check for execution status here: https://dev.azure.com/ML-Governance/ML%20Governance%20V2/_build?definitionId=6&_a=summary')