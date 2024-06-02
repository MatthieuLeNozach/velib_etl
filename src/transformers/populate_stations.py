
from utils.database.database import get_db_session
from utils.database.models import Station

import logging
logger = logging.getLogger(__name__)


def populate_stations(data, *args, **kwargs):
    session = get_db_session()()
    for record in data:
        station = Station(
            record_timestamp=record['record_timestamp'],
            stationcode=record['stationcode'],
            ebike=record['ebike'],
            mechanical=record['mechanical'],
            duedate=record['duedate'],
            numbikesavailable=record['numbikesavailable'],
            numdocksavailable=record['numdocksavailable'],
            capacity=record['capacity'],
            is_renting=record['is_renting'],
            is_installed=record['is_installed'],
            is_returning=record['is_returning'],
        )
        session.merge(station)
    session.commit()
    return data




