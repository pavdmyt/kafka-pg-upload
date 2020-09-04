import asyncio
import signal
import ssl
import sys

import asyncpg
import environs
from confluent_kafka import Consumer

from .config import parse_config
from .kafka_consumer import consume
from .logger import consumer_log, log
from .pg_producer import produce


__version__ = "0.1.0"


async def main() -> None:
    """Main logic.

    Implements programm's flow.
    """
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
    client_conf = {
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
        client_conf.update(auth_conf)

    kafka_client = Consumer(
        client_conf,
        logger=consumer_log,
    )

    # Establish PostgreSQL connection
    #
    if conf.pg_enable_ssl:
        ssl_ctx = ssl.create_default_context(cafile=conf.pg_ssl_ca)
    else:
        ssl_ctx = False

    pg_conf = dict(
        host=conf.pg_host,
        port=conf.pg_port,
        user=conf.pg_user,
        password=conf.pg_password,
        database=conf.pg_db_name,
        timeout=conf.pg_conn_timeout,
        command_timeout=conf.pg_command_timeout,
        ssl=ssl_ctx,
    )
    log.info("connecting to postgresql", **pg_conf)
    try:
        pg_conn = await asyncpg.connect(**pg_conf)
    except (asyncpg.exceptions.PostgresError, OSError) as err:
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


def run() -> None:
    """Entry point for the built executable."""
    # TODO: signals should be registered on event loop, with proper handling
    #       for each of them.
    # Temporary solution.
    signal.signal(signal.SIGINT, lambda signal, frame: sys.exit(0))

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except asyncio.exceptions.CancelledError:
        print({"error": "tasks has been cancelled"})
    finally:
        loop.close()
