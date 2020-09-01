import asyncio
import signal
import sys

import environs
from confluent_kafka import Consumer

from . import __version__
from .config import parse_config
from .consumer import consume
from .logger import consumer_log, log


async def main():
    # Get Config
    try:
        conf = parse_config()
    except environs.EnvValidationError as err:
        log.error(error=err)
        sys.exit(1)

    # Start
    log.info(
        bin=sys.argv[0],
        version=__version__,
        config=conf,
    )

    # Instantiate Kafka consumer
    # Configuration options:
    # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    kafka_client = Consumer(
        # XXX: production setup should communicate via SSL
        {
            "bootstrap.servers": conf.kafka_broker_list,
            "group.id": conf["consumer_group.id"],
            "auto.offset.reset": conf["consumer_auto.offset.reset"],
        },
        logger=consumer_log,
    )

    queue = asyncio.Queue()
    consumers = asyncio.create_task(
        consume(kafka_client, conf, queue, logger=log)
    )
    await asyncio.gather(consumers)


def run():
    # Handle Ctrl+C
    signal.signal(signal.SIGINT, lambda signal, frame: sys.exit(0))
    asyncio.run(main())
