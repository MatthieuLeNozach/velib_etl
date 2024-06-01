import requests

import logging
logger = logging.getLogger(__name__)


def load_data_from_api(*args, **kwargs):
    logger.info("Fetching data from API...")
    url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&timezone=Europe/Paris&rows=2000"
    try:
        response = requests.get(url, headers={"accept": "application/json"})
        response.raise_for_status()
        data = response.json()
        logger.info("Data fetched successfully.")
        return data
    except requests.exceptions.RequestException as e:
        logger.error("Failed to fetch data from API: %s", e)
        raise

def test_output(data, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert data is not None, 'The output is undefined'
