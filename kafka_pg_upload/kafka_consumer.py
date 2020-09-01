"""Kafka consumer.

"""

from asyncio import Queue, sleep

from confluent_kafka import Consumer

from .config import DotDict


async def consume(
    client: Consumer, conf: DotDict, queue: Queue, logger
) -> None:
    client.subscribe([conf.kafka_topic])

    # Process messages
    try:
        while True:
            # timeout (float) – Maximum time to block waiting for message,
            # event or callback (Seconds)
            msg = client.poll(timeout=1.0)

            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                logger.info("waiting for message or event/error in poll()")
                await sleep(conf.consumer_sleep_interval)
                continue

            err = msg.error()
            if err:
                logger.error(error=err)
                continue

            # Check for Kafka message
            record_key = msg.key()
            record_val = msg.value()
            await queue.put(record_val)

            logger.info("record consumed", key=record_key)
    finally:
        # Leave group and commit final offsets
        client.close()
