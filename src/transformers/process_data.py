
import logging
logger = logging.getLogger(__name__)

def process_data(data, *args, **kwargs):
    records = data.get("records", [])
    processed_data = []
    for record in records:
        fields = record.get("fields", {})
        processed_data.append({
                    "record_timestamp": record.get("record_timestamp", ""),
                    "stationcode": fields.get("stationcode", ""),
                    "ebike": fields.get("ebike", 0),
                    "mechanical": fields.get("mechanical", 0),
                    "duedate": fields.get("duedate", ""),
                    "numbikesavailable": fields.get("numbikesavailable", 0),
                    "numdocksavailable": fields.get("numdocksavailable", 0),
                    "capacity": fields.get("capacity", 0),
                    "is_renting": fields.get("is_renting", ""),
                    "is_installed": fields.get("is_installed", ""),
                    "is_returning": fields.get("is_returning", "")
        })
    logger.info("Data processing completed. Processed %d records.", len(processed_data))
    return processed_data



def test_output(processed_data) -> None:
    """
    Template code for testing the output of the block.
    """
    assert processed_data is not None, 'The output is undefined'
