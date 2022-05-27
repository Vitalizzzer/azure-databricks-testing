import requests
import json
from src.helper.request_helper import *
from src.helper.file_helper import *


BASE_URL = "https://adb-4666008530688960.0.azuredatabricks.net/api/2.0/workspace"


def list_workspace_items(context, user):
    url = BASE_URL + "/list"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json"
    }
    json_body = {
        "path": "/Users/" + user
    }

    response = requests.get(url, json=json_body, headers=headers)
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for list workspace items: {parsed_response}")
    return parsed_response


def get_workspace_item_status(context, user, item):
    url = BASE_URL + "/get-status"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json"
    }
    json_body = {
        "path": "/Users/" + user + "/" + item
    }

    response = requests.get(url, json=json_body, headers=headers)
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for list workspace items: {parsed_response}")
    return parsed_response


def create_folder(context, user, folder_path):
    url = BASE_URL + "/mkdirs"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json"
    }
    json_body = {
        "path": "/Users/" + user + "/" + folder_path
    }

    response = requests.post(url, json=json_body, headers=headers)
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for list workspace items: {parsed_response}")
    return parsed_response


def delete_workspace_item(context, user, item_to_delete, recursive):
    url = BASE_URL + "/delete"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json"
    }
    json_body = {
        "path": "/Users/" + user + "/" + item_to_delete,
        "recursive": recursive
    }

    response = requests.post(url, json=json_body, headers=headers)
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for list workspace items: {parsed_response}")
    return parsed_response


def export_notebook(context, user, notebook, save_path):
    url = BASE_URL + "/export"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json"
    }
    data = {
        "path": "/Users/" + user + "/" + notebook,
        "format": "SOURCE",
        "direct_download": "true"
    }

    response = requests.get(url, json=data, headers=headers)
    logging.info(f"Response: {response.text}")
    check_response_status(response)
    with open(save_path, "w") as f:
        f.write(response.text)
    return save_path


def import_notebook(context, user, notebook, notebook_to_import_zip_path):
    encoded = encode_to_base64(notebook_to_import_zip_path)
    decoded = base64_to_string(encoded)
    url = BASE_URL + "/import"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json"
    }
    json_body = {
        "path": "/Users/" + user + "/" + notebook,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": decoded,
        "overwrite": "true"
    }

    response = requests.post(url, json=json_body, headers=headers)
    logging.info(f"Response: {response.text}")
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for list workspace items: {parsed_response}")
    return parsed_response
