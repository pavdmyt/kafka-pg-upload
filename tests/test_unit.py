import asyncio
import json

import pytest
from async_timeout import timeout

from kafka_pg_upload.kafka_consumer import consume
from kafka_pg_upload.logger import log


@pytest.mark.asyncio
async def test_kafka_consumer(
    produce_to_kafka, kafka_consumer, app_conf, conf_topic, messages
):
    # Use proper test topic
    app_conf.kafka_topic = conf_topic

    queue = asyncio.Queue()
    try:
        async with timeout(5):
            await consume(
                client=kafka_consumer, conf=app_conf, queue=queue, logger=log
            )
    except asyncio.TimeoutError:
        pass

    assert queue.qsize() == len(messages)

    records = []
    while queue.qsize() > 0:
        rec_bytes = queue.get_nowait()
        records.append(json.loads(rec_bytes))

    for msg, rec in zip(messages, records):
        assert msg == rec
