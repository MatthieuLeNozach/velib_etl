
from utils.database.database import get_db_session
from utils.database.models import Station

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from utils.database.models import Location

import logging
logger = logging.getLogger(__name__)


@transformer
def populate_stations(data, *args, **kwargs):
    session = get_db_session()()
    logger.info("DB session opened, populating locations...")
    for record in data:
        logger.debug("Processing record: %s", record)
        location = Location(
            stationcode=record['stationcode'],
            name=record['name'],
            latitude=record['latitude'],
            longitude=record['longitude'],
            nom_arrondissement_communes=record['nom_arrondissement_communes'],
        )
        session.merge(location)
    session.commit()
    return data

    


@test
def test_output(data) -> None:
    """
    Template code for testing the output of the block.
    """
    assert data is not None, 'The output is undefined'
