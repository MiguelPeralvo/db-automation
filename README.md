[![Build Status](https://dev.azure.com/db-automation/automation/_apis/build/status/MiguelPeralvo.db-automation?branchName=master)](https://dev.azure.com/db-automation/automation/_build/latest?definitionId=1&branchName=master)

# Introduction 

This project demonstrates the use of Azure DevOps as the tool to work with ML Pipelines in Azure Databricks.-

Workflow Steps:

- Azure DevOps Pipeline (Build Stage) gets triggered via a commit (e.g. as in the train_model.py script). Azure DevOps runs the Build job in the Build stage and does the following:
    - Builds the model (optional) using train_model.py.
    - 

- Azure DevOps Pipeline (Release Stage) / staging job gets triggered via a pull request into release
 
    - Deploy the model into staging using the mlFlow model registry.
    
- Azure DevOps Pipeline (Release Stage) / production job gets triggered via a merge into release:

    - Deploy the model into production using the mlFlow model registry.

