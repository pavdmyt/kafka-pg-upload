"""Write metrics data into PostgreSQL.

"""
import asyncio
import json

from asyncpg.exceptions import InterfaceError, PostgresConnectionError


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


async def produce(conn, conf, queue, logger):
    # Create table
    await conn.execute(_create_table_query(conf.pg_table_name))

    # Main loop
    try:
        while True:
            msg_bytes = await queue.get()
            msg = json.loads(msg_bytes)
            logger.info("writing metric to PostgreSQL")
            query = _compose_insert_query(conf.pg_table_name, msg)
            try:
                await conn.execute(query)
            # Handle losing connection to DB
            except (InterfaceError, PostgresConnectionError) as err:
                logger.error(error=err)
                break
            else:
                queue.task_done()
    finally:
        logger.info(
            "closing connection",
            host=conf.pg_host,
            port=conf.pg_port,
            database=conf.pg_db_name,
        )
        await conn.close()
        for task in asyncio.Task.all_tasks():
            logger.info("cancelling task", task=task)
            task.cancel()
