#!/usr/bin/python3
from azure.common.credentials import get_azure_cli_credentials
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import argparse
import time

def print_activity_run_details(activity_run):
    """Print activity run details."""
    now = datetime.utcnow()

    print(f"{now} - Activity run status: {activity_run.status}")
    if activity_run.status in ['Succeeded', 'InProgress']:
        print(f"{now} - activity_run: {activity_run}")
    else:
        print(f"{now} - Errors: {activity_run.error}")


def main():
    parser = argparse.ArgumentParser(description="Library path in ADF")
    parser.add_argument("-r", "--resource_group", help="Resource group", required=True)
    parser.add_argument("-a", "--adf_name", help="ADF NAME", required=True)
    parser.add_argument("-p", "--adf_pipeline_name", help="ADF pipeline name", required=True)
    parser.add_argument("-o", "--output_file_path", help="Output file path", required=True)
    parser.add_argument("-pa", "--parameters", help="Parameters",
                        required=False)
    args = parser.parse_args()

    resource_group = args.resource_group
    adf_name = args.adf_name
    adf_pipeline_name = args.adf_pipeline_name
    output_file_path = args.output_file_path
    parameters = args.parameters

    print(f"-resource_group is {resource_group}")
    print(f"-adf_name is {adf_name}")
    print(f"-adf_pipeline_name is {adf_pipeline_name}")
    print(f"-output_file_path is {output_file_path}")
    print(f"-parameters is {parameters}")
    credentials, subscription_id = get_azure_cli_credentials()

    # The data factory name. It must be globally unique.

    get_azure_cli_credentials()
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    # Create a pipeline run
    run_response = adf_client.pipelines.create_run(resource_group, adf_name, adf_pipeline_name, parameters=parameters)

    # Monitor the pipeline run
    time.sleep(5)
    pipeline_run = adf_client.pipeline_runs.get(
        resource_group, adf_name, run_response.run_id)
    print("\n\tPipeline run status: {}".format(pipeline_run.status))
    filter_params = RunFilterParameters(
        last_updated_after=datetime.utcnow() - timedelta(1), last_updated_before=datetime.utcnow() + timedelta(1))
    query_response = adf_client.activity_runs.query_by_pipeline_run(
        resource_group, adf_name, pipeline_run.run_id, filter_params)

    while query_response.value[0].status in ['InProgress']:
        print_activity_run_details(query_response.value[0])
        time.sleep(3)
        query_response = adf_client.activity_runs.query_by_pipeline_run(
            resource_group, adf_name, pipeline_run.run_id, filter_params)

    print_activity_run_details(query_response.value[0])


if __name__ == "__main__":
    # Start the main method
    main()
