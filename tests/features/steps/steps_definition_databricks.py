import logging
from behave import *
import src.client.sql_client.databricks_client as databricks
import src.client.api_client.job as job
import src.client.api_client.workspace as workspace
import src.client.api_client.cluster as cluster
import src.client.api_client.dbfs as dbfs

logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')


@Given('upload a file from "{local_file_path}" to "{dbfs_path}" and overwrite = "{overwrite}')
def upload_file(context, local_file_path, dbfs_path, overwrite):
    logging.info("Start testing")
    dbfs.put_file(context, local_file_path, dbfs_path, overwrite)


@Given('delete a file from "{dbfs_path}"')
def delete_file(context, dbfs_path):
    logging.info("Start deleting")
    dbfs.delete(context, dbfs_path)


@Given('import notebook with name = "{name}" and location = "{location}"')
def import_notebook_with_name_and_location(context, name, location):
    workspace.import_notebook(context, context.user, name, location)
    context.notebook_name = name


@Given('create folder with name = "{folder_name}"')
def create_folder_with_name(context, folder_name):
    workspace.create_folder(context, context.user, folder_name)
    context.folder_name = folder_name


@Given('delete notebook with name = "{name}" and recursive = "{recursive}"')
def delete_notebook(context, name, recursive):
    workspace.delete_workspace_item(context, context.user, name, recursive)


@Given('delete job with name = "{name}"')
def delete_current_job(context, name):
    job_id = job.get_job_id_by_name(context, name)
    job.delete_job(context, job_id)


@Given('create a job with name = "{name}", task key = "{task_key}", cluster name = "{cluster_name}" and notebook = "{'
       'notebook}"')
def create_job_(context, name, task_key, cluster_name, notebook):
    notebook_path = f"/Users/{context.user}/{notebook}"
    cluster_id = cluster.get_cluster_id_by_name(context, cluster_name)
    job_id = job.create_job(context, name, task_key, cluster_id, notebook_path, 2)
    assert job_id != "" or int(job_id) > 0, f"Job {name} fails to be created"
    context.job_id = job_id


@When('run job with name = "{name}"')
def run_job_with_name(context, name):
    job_id = job.get_job_id_by_name(context, name)

    run_info = job.trigger_job_run(context, job_id)
    context.job_run_id = run_info.get('run_id')


@When('run job')
def run_job(context):
    run_id = job.trigger_job_run(context, context.job_id)
    context.job_run_id = run_id


@Then('result state = "{result_state}" with max wait time {wait_time} seconds')
def verify_result_state_with_max_wait_time(context, result_state, wait_time):
    retry_count = 20
    period = int(wait_time)/20
    response = job.get_single_job_run(context, context.job_run_id, period, retry_count)
    actual_result_state = job.get_result_state(response)
    assert str(actual_result_state) == result_state


@Given('create table name = "{table_name}" from file = "{file_path}" with format = "{file_format}" '
       'and headers = "{with_headers}"')
def created_table_name_from_csv(context, table_name, file_format, file_path, with_headers):
    context.table_name = table_name
    databricks.create_table(context.connection, context.table_name, file_format, file_path, with_headers)


@Then('number of duplications is "{duplicates_number}"')
def number_of_duplications(context, duplicates_number):
    result = databricks.select_duplicates(context.connection, context.table_name)
    actual_duplicates = str(result["DuplicateRecords"])
    assert actual_duplicates == duplicates_number, "Number of duplicates is incorrect. Expected: {} Actual: {}"\
        .format(duplicates_number, actual_duplicates)


@Then('number of NULL values is "{null_values_number}"')
def number_of_null_values(context, null_values_number):
    result = databricks.select_null_values(context.connection, context.table_name)
    actual_null_values = str(result["NullValues"])
    assert actual_null_values == null_values_number, "Number of NULL values is incorrect. Expected: {} Actual: {}"\
        .format(null_values_number, actual_null_values)
