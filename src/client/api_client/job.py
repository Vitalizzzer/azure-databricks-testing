import requests
import json
import time
from src.helper.request_helper import *


BASE_URL = "https://adb-4666008530688960.0.azuredatabricks.net/api/2.1/jobs"


def create_job(context, name, task_key, cluster_id, notebook_path, max_retries):
    logging.info(f"Start creating a job with name={name}, task_key={task_key}, cluster_id={cluster_id} "
                 f"notebook_path={notebook_path} and max retries={max_retries}")
    url = BASE_URL + "/create"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Content-Type": "application/json"
    }
    json_body = {
        "name": name,
        "tags": {},
        "tasks": [{
            "task_key": task_key,
            "depends_on": [],
            "existing_cluster_id": cluster_id,
            "notebook_task": {
                "notebook_path": notebook_path
            },

            "max_retries": max_retries
        }],
        "max_concurrent_runs": 10,
        "format": "MULTI_TASK"
    }

    response = requests.post(url, json=json_body, headers=headers)
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for create job: {parsed_response}")
    return parsed_response.get("job_id")


def get_all_jobs(context):
    url = BASE_URL + "/list"
    params = {
        "limit": 20,
        "offset": 0,
        "expand_tasks": False
    }
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json"
    }

    response = requests.get(url, params=params, headers=headers)
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for get all jobs: {parsed_response}")
    return parsed_response


def delete_job(context, job_id):
    url = BASE_URL + "/delete"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Content-Type": "application/json"
    }
    json_body = {
        "job_id": job_id
    }

    response = requests.post(url, json=json_body, headers=headers)
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for delete job: {parsed_response}")
    return parsed_response


def get_job_id_by_name(context, job_name):
    all_jobs = get_all_jobs(context)
    logging.info(f"Looking job by name {job_name}")

    for job in all_jobs['jobs']:
        if job['settings']['name'] == job_name:
            logging.info(f"Found by name {job_name}: {job}")
            logging.info(f"job id = {job.get('job_id')}")
            return job.get('job_id')


def trigger_job_run(context, job_id):
    logging.info(f"Start running the job: {job_id}")
    url = BASE_URL + "/run-now"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    json_body = {
        "job_id": int(job_id)
        # "idempotency_token": "8f018174-4792-40d5-bcbc-3e6a527352c8" #Optional unique value e.g. to rerun the job
    }
    response = requests.post(url, json=json_body, headers=headers)
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for trigger job run: {parsed_response}")
    return parsed_response.get("run_id")


def get_single_job_run(context, run_id, wait_time, retry_count):
    query_url = BASE_URL + "/runs/get"
    params = {
        "run_id": run_id
    }
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json"
    }

    response = requests.get(query_url, params=params, headers=headers)
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for get single job run: {parsed_response}")

    counter = retry_count
    while parsed_response['state'].get('life_cycle_state') != 'TERMINATED':
        time.sleep(wait_time)
        response = requests.get(query_url, params=params, headers=headers)
        check_response_status(response)
        parsed_response = json.loads(response.text)
        logging.info(f"Parsed response for get single job run: {parsed_response}")
        counter -= counter
        if parsed_response['state'].get('life_cycle_state') == 'TERMINATED':
            return parsed_response


def get_result_state(response):
    logging.info("Getting job run result state.")
    result_state = response['state'].get('result_state')
    logging.info(f"Found job run result state: {result_state}")
    return result_state
