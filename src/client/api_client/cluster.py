import requests
import json
from src.helper.request_helper import *

BASE_URL = "https://adb-4666008530688960.0.azuredatabricks.net/api/2.0/clusters"


def get_all_clusters(context):
    url = BASE_URL + "/list"
    headers = {
        "Authorization": "Bearer " + context.access_token,
        "Accept": "application/json"
    }

    response = requests.get(url, headers=headers)
    check_response_status(response)
    parsed_response = json.loads(response.text)
    logging.info(f"Parsed response for get all clusters: {parsed_response}")
    return parsed_response


def get_cluster_id_by_name(context, cluster_name):
    all_clusters = get_all_clusters(context)
    logging.info(f"Looking for cluster by name {cluster_name}")
    for cluster in all_clusters['clusters']:
        if cluster['cluster_name'] == cluster_name:
            logging.info(f"Found cluster id = {cluster.get('cluster_id')}")
            return cluster.get('cluster_id')
