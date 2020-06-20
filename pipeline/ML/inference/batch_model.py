import argparse
import os
import requests
import mlflow
import mlflow.sklearn
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
    # parser.add_argument("-p", "--phase", help="Phase", default="qa", required=True)

    args = parser.parse_args()
    model_name = args.model_name
    home = args.root_path
    stage = args.stage
    db = args.db_name.replace("@", "_").replace(".", "_")
    ml_output_predictions_table = args.table_name

    print(f"Model name: {model_name}")
    print("batch_inference")


if __name__ == '__main__':
    main()
    # sys.exit(0)
