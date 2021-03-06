import argparse
import os
import requests
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
from mlflow.tracking import artifact_utils
from mlflow import pyfunc
import json
from pyspark.sql.functions import col
# import sys

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
    parser = argparse.ArgumentParser(description="Deploy and test batch model")
    parser.add_argument("-m", "--model_name", help="Model name", required=False)
    parser.add_argument("-r", "--root_path", help="Prefix path", required=False)
    parser.add_argument("-s", "--stage", help="Stage", default="staging", required=False)
    parser.add_argument("-d", "--db_name", help="Output Database name", default="wine", required=False)
    parser.add_argument(
        "-t", "--table_name", help="Output Table name", default="mlops_wine_quality_regression",
                        required=False)
    parser.add_argument(
        "-p", "--model_path", help="Model's artifacts path",
        default="/dbfs/FileStore/Shared/db-automation/mlflow/wine-model", required=True)

    args = parser.parse_args()
    model_name = args.model_name
    home = args.root_path
    stage = args.stage.replace(" ", "")
    db = args.db_name.replace("@", "_").replace(".", "_")
    ml_output_predictions_table = args.table_name
    model_path = args.model_path

    print(f"Model name: {model_name}")
    print(f"home: {home}")
    print(f"stage: {stage}")
    print(f"db: {db}")
    print(f"ml_output_predictions_table: {ml_output_predictions_table}")
    print(f"model_path: {model_path}")
    print("batch_inference")

    temp_data_path = f"/dbfs/tmp/mlflow-wine-quality.csv"
    data_uri = "https://raw.githubusercontent.com/mlflow/mlflow/master/examples/sklearn_elasticnet_wine/wine-quality.csv"
    dbfs_wine_data_path = download_wine_file(data_uri, home, temp_data_path)
    wine_df = spark.read.format("csv").option("header", "true").load(dbfs_wine_data_path).drop("quality").cache()
    wine_df = wine_df.select(*(col(column).cast("float").alias(column.replace(" ", "_")) for column in wine_df.columns))
    data_spark = wine_df
    model_artifact = 'model'
    artifact_uri = model_path
    print(f"artifact_uri: {artifact_uri}")
    model_uri = f"{artifact_uri}/{model_artifact}"
    print(f"model_uri: {model_uri}")
    udf = pyfunc.spark_udf(spark, model_uri)

    # data_spark = spark.read.csv(dbfs_wine_data_path, header=True)
    predictions = data_spark.select(udf(*data_spark.columns).alias('prediction'), "*")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    spark.sql(f"DROP TABLE IF EXISTS {db}.{ml_output_predictions_table}")
    predictions.write.format("delta").mode("overwrite").saveAsTable(f"{db}.{ml_output_predictions_table}")
    output = json.dumps({
        "model_name": model_name,
        "model_uri": model_uri
    })

    print(output)


if __name__ == '__main__':
    main()
    # sys.exit(0)
