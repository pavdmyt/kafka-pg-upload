"""Write metrics data into PostgreSQL."""
import asyncio
import json
import ssl
from typing import Any, Tuple, Union

import asyncpg

from .config import DotDict


async def connect_pg(conf: DotDict, logger) -> asyncpg.Connection:
    """Create PostgreSQL connection."""
    ssl_ctx = (
        ssl.create_default_context(cafile=conf.pg_ssl_ca)
        if conf.pg_enable_ssl
        else False
    )
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
    logger.info("connecting to PostgreSQL", config=pg_conf)
    return await asyncpg.connect(**pg_conf)


def _compose_insert_query(table_name: str, msg: dict) -> str:
    """Build SQL query to insert row with metrics data into a table."""
    url = msg["page_url"]
    code = int(msg["http_code"])
    resp_time = int(msg["response_time"])
    ts = msg["ts"]
    query = (
        f"INSERT INTO {table_name}(page_url, http_code, response_time, timestamp) "
        f"VALUES('{url}', {code}, {resp_time}, '{ts}')"
    )
    return query


def _create_table_query(table_name: str) -> str:
    """Build SQL query to create metrics table."""
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name}("
        f"    id SERIAL PRIMARY KEY,"
        f"    page_url TEXT,"
        f"    http_code SMALLINT,"
        f"    response_time INT,"
        f"    timestamp TIMESTAMPTZ"
        f")"
    )


def _handle_exc(
    results: Tuple[Union[Any, BaseException]],
    conn_queue: asyncio.Queue,
    logger,
):
    for res in results:
        if isinstance(res, Exception):
            logger.error("failed to connect to PostgreSQL", error=res)
            asyncio.create_task(conn_queue.put(None))
            raise res  # trigger global exception handler


async def produce(
    conf: DotDict, queue: asyncio.Queue, conn_queue: asyncio.Queue, logger
) -> None:
    """
    Reads messages from the queue, build rows and insert them into PG table.

    Gracefully cancels async tasks and shutdown event loop in case of
    connectivity issues with PostgreSQL. It is assumed that service failures
    should be handled by container orchestration software (it is easy in
    Kubernetes).

    """
    # If return_exceptions=True, exceptions are treated the same as successful
    # results, and aggregated in the result list
    conn_or_exc = await asyncio.gather(
        connect_pg(conf, logger), return_exceptions=True
    )
    _handle_exc(conn_or_exc, conn_queue, logger)

    # Will execute if there were no exceptions at establishing connection
    conn = conn_or_exc[0]
    # Using a dedicated queue to share PG connection with `shutdown`
    # to properly clean up
    asyncio.create_task(conn_queue.put(conn))

    # Create table
    await conn.execute(_create_table_query(conf.pg_table_name))  # type: ignore

    while True:
        msg_bytes = await queue.get()
        msg = json.loads(msg_bytes)
        logger.info("writing metric to PostgreSQL")
        query = _compose_insert_query(conf.pg_table_name, msg)
        await conn.execute(query)  # type: ignore
        queue.task_done()
