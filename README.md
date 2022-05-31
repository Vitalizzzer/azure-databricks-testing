# Azure Databricks Testing POC

The test suite executes tests in Azure Databricks.

## Configuration

### Add environment variables for Azure Databricks in terminal:

For UNIX like platforms:
```
export SERVER_HOSTNAME=<SERVER_HOSTNAME>
export HTTP_PATH=<HTTP_PATH>
export DATABRICKS_TOKEN=<ACCESS_TOKEN>
export USER=<username@ah.nl>
```

For Windows platform:
```
set SERVER_HOSTNAME=<SERVER_HOSTNAME>
set HTTP_PATH=<HTTP_PATH>
set DATABRICKS_TOKEN=<ACCESS_TOKEN>
set USER=<username@ah.nl>
```

### Add environment variables for Report Portal in terminal (optional):

For UNIX like platforms:
```
export RP_UUID=<UUID>
export RP_ENDPOINT=<REPORT_PORTAL_ADDRESS>
export RP_PROJECT=<PROJECT_NAME>
export RP_LAUNCH=<LAUNCH_NAME>
```

For Windows platform:
```
set RP_UUID=<UUID>
set RP_ENDPOINT=<REPORT_PORTAL_ADDRESS>
set RP_PROJECT=<PROJECT_NAME>
set RP_LAUNCH=<LAUNCH_NAME>
```

## Execution
### Run the command in the terminal:

* With behave html report:
```bash 
behave -f html -o report/behave-report.html -D rp_enable=False -D step_based=True --tags=Databricks
```

* With behave json report:
```bash 
behave -f json.pretty -o report/behave-report.json -D rp_enable=False -D step_based=True --tags=Databricks
```

* With junit xml report:
```bash 
behave --junit --junit-directory report -D step_based=True --tags=Databricks
```

* With Allure report:
```
behave -f allure_behave.formatter:AllureFormatter -o report/allure-report -D step_based=True --tags=Databricks  
```

***Note:*** to enable publishing results to Report Portal set ```rp_enable=True``` in the command above.

## Reporting
* Execution results are in ***report*** folder.  
* To open Allure report: 
```
allure serve report/allure-report
```

## Publish results to Zephyr Scale

***Detailed guidance to zephyr-results-publisher tool: [Zephyr Scale API - Automation Results Publisher](https://pypi.org/project/zephyr-results-publisher/)***

1. Set environment variable Zephyr Scale API_KEY:  
How to generate API KEY: [Generating API Access Tokens](https://support.smartbear.com/zephyr-scale-cloud/docs/rest-api/generating-api-access-tokens.html)  

For UNIX like platforms:
```
export API_KEY=XXXXXXXXX
```

For Windows platforms:
```
set API_KEY=XXXXXXXXX
```

2. Open tests -> features -> environment.py
3. Create test cycle and publish results into Test Cycle root folder use function:
**Notes: Please, set needed values for the parameters. ```report_format=``` should be ```"behave"``` in order to use Cucumber BDD report format.** 

```
@atexit.register
def publish_report():
    if os.environ.get("API_KEY") is not None:
        project_key = "PROJECT_KEY"
        source_report_file = "report/behave-report.json"
        report_format = "behave"
        auto_create_test_cases = "true"
        publisher.publish(project_key, source_report_file, report_format, auto_create_test_cases)
```
or to publish automation results into a specific Zephyr Test Cycle folder and customize Test Cycle properties:
```
@atexit.register
def publish_report():
    if os.environ.get("API_KEY") is not None:
        project_key = "PROJECT_KEY"
        source_report_file = "report/behave-report.json"
        report_format = "behave"
        auto_create_test_cases = "true"
        test_cycle_name = "Test Cycle Description"
        test_cycle_folder_name = "Already created folder name"
        test_cycle_description = "Test Cycle description"
        test_cycle_jira_project_version = 1
        test_cycle_custom_fields = {}
        publisher.publish_customized_test_cycle(project_key,
                                                source_report_file,
                                                report_format,
                                                auto_create_test_cases,
                                                test_cycle_name,
                                                test_cycle_folder_name,
                                                test_cycle_description,
                                                test_cycle_jira_project_version,
                                                test_cycle_custom_fields)
```
