# Introduction 

This project demonstrates the use of Azure DevOps as the tool to work with ML Pipelines in Azure Databricks.-

Workflow Steps

- Data Scientist promotes model into Staging (e.g. as in the train_wine_model Notebook)
- Pipeline gets triggered (manually or via API, as in the train_wine_model Notebook)
- Azure Dev Ops uploads deployment notebook (deploy_azure_ml_model) from git to a dedicated Test/QA region within the workspace via the Databricks workspace API
- Azure Dev Ops runs deploy notebook with creater job and run submit which does the following:
    - Retrieves latest model staging from registry
    - Deploys model as an Azure ML model and creates an image
    - Deploys REST API for he model/image
    - returns an the REST API deployment URL to Azure Dev Ops
- Azure Dev Ops uploads test notebook from git to a dedicated Test/QA region within the workspace
- Azure Dev Ops runs deploy notebook with run submit which does the following:
    - Retrieves test data
    - Invokes REST API
- If successful, DevOps will deploy the model into production using the mlFlow REST API

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Build and Test
TODO: Describe and show how to build your code and run the tests. 

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 
