import requests
import json
from src.helper.request_helper import *
from src.helper.file_helper import *


BASE_URL = "https://adb-4666008530688960.0.azuredatabricks.net/api/2.0/dbfs"


def put_file(context, local_file_path, dbfs_path, overwrite):
    encoded = encode_to_base64(local_file_path)
    decoded = base64_to_string(encoded)
    url = BASE_URL + "/put"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json"
    }
    json_body = {
        "contents": decoded,
        "path": dbfs_path,
        "overwrite": overwrite
    }

    response = requests.post(url, json=json_body, headers=headers)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for put a file: {parsed_response}")
    return parsed_response


def delete(context, dbfs_path):
    url = BASE_URL + "/delete"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json"
    }
    json_body = {
        "path": dbfs_path
    }

    response = requests.post(url, json=json_body, headers=headers)
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for delete a file: {parsed_response}")
    return parsed_response
