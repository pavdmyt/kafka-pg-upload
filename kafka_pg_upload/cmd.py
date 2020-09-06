import asyncio
import functools
import signal
import sys

import environs
from confluent_kafka import Consumer

from .config import parse_config
from .kafka_consumer import consume, new_consumer
from .logger import log
from .pg_producer import produce


__version__ = "0.1.0"


async def shutdown(
    loop,
    conn_queue: asyncio.Queue,
    kafka_client: Consumer,
    _signal=None,
) -> None:
    """Cleanup tasks tied to the service's shutdown."""
    if _signal:
        log.info("received exit signal", signal=_signal.name)

    log.info("close down and terminate the Kafka consumer")
    kafka_client.close()
    log.info("closing PostgreSQL connection")
    pg_conn = await conn_queue.get()
    await pg_conn.close()

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()

    log.info(f"cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


def handle_exceptions(
    loop, ctx, conn_queue: asyncio.Queue, kafka_client: Consumer
) -> None:
    """
    Custom exception handler for event loop.

    Context is a dict object containing the following keys:

    ‘message’:  Error message;
    ‘exception’ (optional): Exception object;
    ‘future’    (optional): asyncio.Future instance;
    ‘handle’    (optional): asyncio.Handle instance;
    ‘protocol’  (optional): Protocol instance;
    ‘transport’ (optional): Transport instance;
    ‘socket’    (optional): socket.socket instance.

    https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.call_exception_handler

    """
    # context["message"] will always be there; but context["exception"] may not
    msg = ctx.get("exception", ctx["message"])
    log.error("caught exception", exception=msg)
    log.info("Shutting down...")
    asyncio.create_task(shutdown(loop, conn_queue, kafka_client))


def run() -> None:
    """Entry point for the built executable."""
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

    # Instantiate clients
    kafka_client = new_consumer(conf)
    queue = asyncio.Queue()
    conn_queue = asyncio.Queue()  # used to share PG connection

    # Signals handling
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s,
            lambda s=s: asyncio.create_task(
                shutdown(loop, conn_queue, kafka_client, _signal=s)
            ),
        )

    # Exception handler must be a callable with the signature matching
    # (loop, context)
    # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.set_exception_handler
    exc_handler = functools.partial(
        handle_exceptions, conn_queue=conn_queue, kafka_client=kafka_client
    )
    loop.set_exception_handler(exc_handler)

    try:
        loop.create_task(consume(kafka_client, conf, queue, log))
        loop.create_task(produce(conf, queue, conn_queue, log))
        loop.run_forever()
    finally:
        loop.close()
        log.info("shutdown successfully")
