import argparse
import requests
import time
from pyspark.sql import SparkSession
import os
import warnings
import json

import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet

import mlflow
import mlflow.sklearn
from pyspark.sql.functions import col


if 'spark' not in locals():
    spark = SparkSession.builder.appName('Test').getOrCreate()


def download_file(data_uri, data_path):
    if os.path.exists(data_path):
        print("File {} already exists".format(data_path))
    else:
        print("Downloading {} to {}".format(data_uri, data_path))
        # rsp = requests.get(data_uri)
        with open(data_path, 'w') as f:
            f.write(requests.get(data_uri).text)


def download_wine_file(data_uri, home, data_path):
    download_file(data_uri, data_path)
    final_path = f"{home}/mlflow/wine-quality/wine-quality.csv"
    print(f"Copying file to {final_path}")
    dbutils.fs.cp(f"/tmp/mlflow-wine-quality.csv", final_path)

    return final_path


def main():
    parser = argparse.ArgumentParser(description="Train model")
    parser.add_argument("-e", "--experiment_name", help="Experiment name", required=True)
    parser.add_argument("-m", "--model_name", help="Model name", required=True)
    parser.add_argument("-r", "--root_path", help="Prefix path", required=True)
    parser.add_argument("-d", "--db_name", help="Output Database name", default="wine", required=False)
    parser.add_argument(
        "-t", "--table_name", help="Wine Table name", default="mlops_wine_quality_input",
        required=False)


    args = parser.parse_args()
    model_name = args.model_name
    home = args.root_path
    experiment_name = args.experiment_name
    db = args.db_name.replace("@", "_").replace(".", "_")
    wine_table = args.table_name

    # data_path = "/dbfs/tmp/mlflow-wine-quality.csv"
    temp_data_path = f"/dbfs/tmp/mlflow-wine-quality.csv"
    data_uri = "https://raw.githubusercontent.com/mlflow/mlflow/master/examples/sklearn_elasticnet_wine/wine-quality.csv"
    dbfs_wine_data_path = download_wine_file(data_uri, home, temp_data_path)
    wine_df = spark.read.format("csv").option("header", "true").load(dbfs_wine_data_path).cache()
    wine_df = wine_df.select(*(col(column).cast("float").alias(column.replace(" ", "_")) for column in wine_df.columns))
    wine_df = wine_df.withColumn("quality", col("quality").cast("integer"))
    spark.sql(f"DROP TABLE IF EXISTS {db}.{wine_table}")
    wine_df.write.format("delta").mode("overwrite").saveAsTable(f"{db}.{wine_table}")
    # wine_data_path = dbfs_wine_data_path.replace("dbfs:", "/dbfs")

    def eval_metrics(actual, pred):
        rmse = np.sqrt(mean_squared_error(actual, pred))
        mae = mean_absolute_error(actual, pred)
        r2 = r2_score(actual, pred)
        return rmse, mae, r2

    def train_model(wine_input_df, model_path, alpha, l1_ratio):
        warnings.filterwarnings("ignore")
        np.random.seed(40)

        # Read the wine-quality csv file (make sure you're running this from the root of MLflow!)
        # data = pd.read_csv(wine_data_path, sep=None)
        data = wine_input_df.toPandas()

        # Split the data into training and test sets. (0.75, 0.25) split.
        train, test = train_test_split(data)

        # The predicted column is "quality" which is a scalar from [3, 9]
        train_x = train.drop(["quality"], axis=1)
        test_x = test.drop(["quality"], axis=1)
        train_y = train[["quality"]]
        test_y = test[["quality"]]

        # Start a new MLflow training run
        with mlflow.start_run() as run:
            print("MLflow:")
            print(f"  run_id: {run.info.run_id}")
            print(f"  experiment_id: {run.info.experiment_id}")
            print(f"  experiment_id: {run.info.artifact_uri}")

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

    print(f"Experiment name: {experiment_name}")
    mlflow.set_experiment(experiment_name=experiment_name)


    alpha_1 = 0.75
    l1_ratio_1 = 0.25
    model_path = 'model'
    # run_id1 = train_model(wine_data_path=wine_data_path, model_path=model_path, alpha=alpha_1, l1_ratio=l1_ratio_1)

    run_id1 = train_model(wine_input_df=wine_df, model_path=model_path, alpha=alpha_1, l1_ratio=l1_ratio_1)
    model_uri = f"runs:/{run_id1}/{model_path}"

    result = mlflow.register_model(
        model_uri,
        model_name
    )
    time.sleep(10)
    version = result.version

    client = mlflow.tracking.MlflowClient()

    client.transition_model_version_stage(
        name=model_name,
        version=version,
        stage="staging")


    output = json.dumps({
        "model_name": model_name,
        "version": version
    })

    print(output)


if __name__ == '__main__':
    main()
