[![Build Status](https://dev.azure.com/db-automation/automation/_apis/build/status/MiguelPeralvo.db-automation?branchName=release)](https://dev.azure.com/db-automation/automation/_build/latest?definitionId=2&branchName=release)

# Introduction 

This project demonstrates the use of MLflow, Azure DevOps and Azure Data Factory as tools to work with ML Pipelines in Azure Databricks.

Workflow Steps:

- Azure DevOps Pipeline (CI/Build Stage) gets triggered via a commit (e.g. as in the train_model.py script). Azure DevOps runs the Build job in the Build stage and does the following:
    - Runs unit tests in the Azure Devops vm.
    - Builds a wheel file
    - Trains the model (optional) using train_model.py and registers it in the MLflow model registry
    - Retrieves the model from the MLflow model registry and tests the model in the Azure Databricks Dev Environment

- Azure DevOps Pipeline CD-Release Stage / Staging job (via PR into the release branch):
 
    - Deploy the model into Databricks staging using the mlFlow model registry.
    - Deploys other artifacts
    - Deploys the Azure Data Factory latest pipeline definition file into ADF Staging
    - Runs the ADF into staging E2E.
     
    
- Azure DevOps Pipeline (Release Stage) / production job gets triggered via a merge into release (via merge into the release branch):

    - Deploy the model into Databricks production using the mlFlow model registry.
    - Deploys other artifacts
    - Deploys the Azure Data Factory latest pipeline definition file into ADF Production

