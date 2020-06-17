#!/usr/bin/python3
from azure.common.credentials import get_azure_cli_credentials
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import argparse
import time


def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, 'location'):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, 'tags'):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, 'properties'):
        print_properties(group.properties)
    print("\n")


def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n")


def print_activity_run_details(activity_run):
    """Print activity run details."""
    print("\n\tActivity run details\n")
    print("\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == 'Succeeded':
        print(f"activity_run: {activity_run}")
    else:
        print("\tErrors: {}".format(activity_run.error['message']))


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
    subscription_id = args.subscription_id
    adf_name = args.adf_name
    adf_pipeline_name = args.adf_pipeline_name
    output_file_path = args.output_file_path
    parameters = args.parameters

    print(f"-resource_group is {resource_group}")
    print(f"-subscription_id is {subscription_id}")
    print(f"-adf_name is {adf_name}")
    print(f"-adf_pipeline_name is {adf_pipeline_name}")
    print(f"-output_file_path is {output_file_path}")
    print(f"-parameters is {parameters}")
    credentials, subscription_id = get_azure_cli_credentials()

    # The data factory name. It must be globally unique.

    get_azure_cli_credentials()
    adf_client = DataFactoryManagementClient(credentials, subscription_id)

    # Create a pipeline run
    run_response = adf_client.pipelines.create_run(resource_group, adf_name, adf_pipeline_name, parameters={parameters})

    # Monitor the pipeline run
    time.sleep(30)
    pipeline_run = adf_client.pipeline_runs.get(
        resource_group, adf_name, run_response.run_id)
    print("\n\tPipeline run status: {}".format(pipeline_run.status))
    filter_params = RunFilterParameters(
        last_updated_after=datetime.now() - timedelta(1), last_updated_before=datetime.now() + timedelta(1))
    query_response = adf_client.activity_runs.query_by_pipeline_run(
        resource_group, adf_name, pipeline_run.run_id, filter_params)
    print_activity_run_details(query_response.value[0])


if __name__ == "__main__":
    # Start the main method
    main()
