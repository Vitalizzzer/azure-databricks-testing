import logging


def check_response_status(response):
    logging.info(f"Response code: {response.status_code}")
    if response.status_code != 200:
        logging.info(f"Error appears: {response.text}")
        raise Exception(response.text)
