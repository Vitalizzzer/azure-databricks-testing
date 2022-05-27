@Databricks
#noinspection CucumberUndefinedStep,CucumberPlusUndefinedStep
Feature: Data verification in Azure Databricks
  SQL and API commands are executed and results are verified

########################################################################################################################
  @SQL @1
  Scenario: Verify absent duplications
    Given upload a file from "resources/data/csv/products.csv" to "/FileStore/tables/products.csv" and overwrite = "true"
    Given create table name = "products" from file = "/FileStore/tables/products.csv" with format = "CSV" and headers = "true"
    Then number of duplications is "0"

########################################################################################################################
  @SQL @2
  Scenario: Verify absent NULL values
    Given upload a file from "resources/data/csv/products.csv" to "/FileStore/tables/products.csv" and overwrite = "true"
    Given create table name = "products" from file = "/FileStore/tables/products.csv" with format = "CSV" and headers = "true"
    Then number of NULL values is "0"

########################################################################################################################
  @API @3
  Scenario Outline: Verify absent duplications E2E
    Given upload a file from "resources/data/csv/products.csv" to "/FileStore/tables/products.csv" and overwrite = "true"
    Given create table name = "products" from file = "/FileStore/tables/products.csv" with format = "CSV" and headers = "true"
    Given create folder with name = "<folder_name>"
    Given import notebook with name = "<notebook_name>" and location = "resources/data/notebooks/verify_absent_duplications.dbc"
    Given create a job with name = "<job_name>", task key = "<task_key>", cluster name = "<cluster_name>" and notebook = "<notebook_name>"
    When run job
    Then result state = "SUCCESS" with max wait time 40 seconds
    Given delete job with name = "<job_name>"
    Given delete notebook with name = "<notebook_name>" and recursive = "false"
    Given delete a file from "/FileStore/tables/products.csv"

    Examples:
      | folder_name | notebook_name                         | job_name                | task_key              | cluster_name |
      | SmokeTests  | SmokeTests/Verify_Absent_Duplications | Verify_Duplications_Job | Duplications_Task_Key | TestCluster  |

########################################################################################################################
  @API @4
  Scenario Outline: Verify absent NULL values E2E
    Given upload a file from "resources/data/csv/products.csv" to "/FileStore/tables/products.csv" and overwrite = "true"
    Given create table name = "products" from file = "/FileStore/tables/products.csv" with format = "CSV" and headers = "true"
    Given create folder with name = "<folder_name>"
    Given import notebook with name = "<notebook_name>" and location = "resources/data/notebooks/verify_absent_null_values.dbc"
    Given create a job with name = "<job_name>", task key = "<task_key>", cluster name = "<cluster_name>" and notebook = "<notebook_name>"
    When run job
    Then result state = "SUCCESS" with max wait time 40 seconds
    Given delete job with name = "<job_name>"
    Given delete notebook with name = "<notebook_name>" and recursive = "false"
    Given delete a file from "/FileStore/tables/products.csv"

    Examples:
      | folder_name | notebook_name                        | job_name               | task_key             | cluster_name |
      | SmokeTests  | SmokeTests/Verify_Absent_Null_Values | Verify_Null_Values_Job | Null_Values_Task_Key | TestCluster  |
