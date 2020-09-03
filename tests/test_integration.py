import json
import multiprocessing as mp
import os
import time

import kafka_pg_upload


def test_e2e(kafka_producer, conf_topic, messages, pg_reader):
    """End-to-end test of the service.

    Write test messages into the Kafka test topic (new created for every
    test session), execute kafka_pg_upload and read PostgreSQL records
    from the test table (new created for every test session).

    Afterwards Kafka message conntents and PostgreSQL records
    are compared.

    """
    # Produce messages into Kafka test topic
    #
    for msg in messages:
        kafka_producer.produce(
            conf_topic,
            key=msg["page_url"],
            value=json.dumps(msg),
        )
    kafka_producer.flush()

    # Run kafka-pg-upload and wait a bit
    #

    # Pass test configuration
    conf_table = conf_topic  # use same name for PG table
    os.environ["KAPG_KAFKA_TOPIC"] = conf_topic
    os.environ["KAPG_PG_TABLE_NAME"] = conf_table

    # Run as separate process
    p = mp.Process(target=kafka_pg_upload.run)
    p.start()
    time.sleep(8)
    p.terminate()

    # Read records from PostgreSQL test table
    #
    try:
        conn = pg_reader
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {conf_table};")
        pg_records = [record for record in cur]
    finally:
        cur.close()
        conn.close()

    # Test messages written to Kafka contain the same data as
    # messages read from PostgreSQL
    assert len(messages) == len(pg_records)
    for i, rec in enumerate(pg_records):
        _, page_url, http_code, resp_time, _ = rec
        assert page_url == messages[i]["page_url"]
        assert http_code == messages[i]["http_code"]
        assert resp_time == messages[i]["response_time"]
