"""Write metrics data into PostgreSQL.

"""
import json


def compose_insert_query(table_name, msg):
    url = msg["page_url"]
    code = int(msg["http_code"])
    resp_time = int(msg["response_time"])
    ts = msg["ts"]
    query = (
        f"INSERT INTO {table_name}(page_url, http_code, response_time, timestamp) "
        f"VALUES('{url}', {code}, {resp_time}, '{ts}')"
    )
    return query


async def produce(conn, conf, queue, logger):
    # Create table
    await conn.execute(
        (
            f"CREATE TABLE IF NOT EXISTS {conf.pg_table_name}("
            f"    id SERIAL PRIMARY KEY,"
            f"    page_url TEXT,"
            f"    http_code SMALLINT,"
            f"    response_time INT,"
            f"    timestamp TIMESTAMPTZ"
            f")"
        )
    )

    # Main loop
    try:
        while True:
            msg_bytes = await queue.get()
            msg = json.loads(msg_bytes)
            logger.info("writing metrics to PostgreSQL")
            query = compose_insert_query(conf.pg_table_name, msg)
            await conn.execute(query)
            queue.task_done()
    finally:
        await conn.close()
