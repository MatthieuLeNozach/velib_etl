
import logging
logger = logging.getLogger(__name__)

def process_data(data, *args, **kwargs):
    records = data.get("records", [])
    processed_data = []
    for record in records:
        fields = record.get("fields", {})
        processed_data.append({
            "name": fields.get("name", ""),
            "stationcode": fields.get("stationcode", ""),
            "latitude": fields.get("coordonnees_geo", [None, None])[0],
            "longitude": fields.get("coordonnees_geo", [None, None])[1],
            "nom_arrondissement_communes": fields.get("nom_arrondissement_communes", "")
        })
    logger.info("Data processing completed. Processed %d records.", len(processed_data))
    return processed_data



def test_output(processed_data) -> None:
    """
    Template code for testing the output of the block.
    """
    assert processed_data is not None, 'The output is undefined'
