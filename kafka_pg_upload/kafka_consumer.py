"""Kafka consumer code."""

from asyncio import Queue, sleep

from confluent_kafka import Consumer

from .config import DotDict
from .logger import consumer_log


def new_consumer(conf: DotDict) -> Consumer:
    """
    Create pre-configured instance of confluent_kafka.Consumer.

    Configuration options:
    https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    """
    basic_conf = {
        "bootstrap.servers": conf.kafka_broker_list,
        "group.id": conf["consumer_group.id"],
        "auto.offset.reset": conf["consumer_auto.offset.reset"],
    }
    if conf.kafka_enable_cert_auth:
        auth_conf = {
            "security.protocol": "ssl",
            "ssl.key.location": conf.kafka_ssl_key,
            "ssl.certificate.location": conf.kafka_ssl_cert,
            "ssl.ca.location": conf.kafka_ssl_ca,
        }
        basic_conf.update(auth_conf)
    return Consumer(basic_conf, logger=consumer_log)


async def consume(
    client: Consumer, conf: DotDict, queue: Queue, logger
) -> None:
    """
    Reads messages from the specified topic.

    Messages are placed into the queue for subsequent processing.
    In case of connectivity failures to Kafka Broker, retries till
    connection is available again.

    """
    client.subscribe([conf.kafka_topic])

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
