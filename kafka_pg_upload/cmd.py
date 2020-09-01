import asyncio
import signal
import sys

import asyncpg
import environs
from confluent_kafka import Consumer

from . import __version__
from .config import parse_config
from .kafka_consumer import consume
from .logger import consumer_log, log
from .pg_producer import produce


async def main():
    # Get Config
    try:
        conf = parse_config()
    except environs.EnvValidationError as err:
        log.error(error=err)
        sys.exit(1)

    # Basic config info
    log.info(
        bin=sys.argv[0],
        version=__version__,
        config=conf,
    )

    # Instantiate Kafka consumer
    #

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

    # Establish PostgreSQL connection
    #
    pg_conf = dict(
        # XXX: production setup should communicate via SSL
        host=conf.pg_host,
        port=conf.pg_port,
        user=conf.pg_user,
        password=conf.pg_password,
        database=conf.pg_db_name,
        timeout=conf.pg_conn_timeout,
    )
    log.info("connecting to postgresql", **pg_conf)
    try:
        pg_conn = await asyncpg.connect(**pg_conf)
    except asyncpg.exceptions.PostgresError as err:
        log.error(error=err)
        sys.exit(1)

    # Initiate consumer-producer pattern
    #
    queue = asyncio.Queue()
    consumers = asyncio.create_task(
        consume(kafka_client, conf, queue, logger=log)
    )
    asyncio.create_task(produce(pg_conn, conf, queue, logger=log))
    await asyncio.gather(consumers)


def run():
    # Handle Ctrl+C
    signal.signal(signal.SIGINT, lambda signal, frame: sys.exit(0))
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except asyncio.exceptions.CancelledError:
        print({"error": "tasks has been cancelled"})
    finally:
        loop.close()
