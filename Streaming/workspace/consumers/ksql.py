"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import sys
import requests

import topic_check


logger = logging.getLogger(__name__)

KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    kafka_topic = 'turnstiles',
    value_format = 'avro',
    key = 'station_id'
);
CREATE TABLE turnstile_summary
WITH (value_format = 'json') AS
    SELECT station_id, COUNT(station_id) AS count
    FROM turnstile
    GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        logger.info("The topic: turnstile_summary already exists")
        return

    logging.info("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    try:
        resp.raise_for_status()
        logging.info("KSQL Query request was successful")
    except Exception as e:
        logging.error("KSQL Query request was not successful - " + str(e))


if __name__ == "__main__":
    execute_statement()
