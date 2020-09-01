from environs import Env


class DotDict(dict):
    """dot.notation access to dict attributes."""

    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


# Store configuration separate from your code, as per The Twelve-Factor App
# methodology.
def parse_config():
    # TODO: add type hints
    env = Env()
    env.read_env()

    config = {
        # Kafka related configuration
        #
        "kafka_broker_list": env.str("KAPG_BROKER_LIST", "localhost:9092,"),
        "kafka_topic": env.str("KAPG_KAFKA_TOPIC", "pagemonitor_metrics"),
        # Client group id string. All clients sharing the same group.id belong
        # to the same group
        "consumer_group.id": env.str("KAPG_GROUP_ID", "42"),
        # Action to take when there is no initial offset in offset store or
        # the desired offset is out of range
        "consumer_auto.offset.reset": env.str(
            "KAPG_AUTOOFFSETRESET", "earliest"
        ),
        "consumer_sleep_interval": env.float("KAPG_CONSUMER_SLEEP", 2.0),
        # PostgreSQL related configuration
        #
        "pg_host": env.str("KAPG_PG_HOST", "localhost"),
        "pg_port": env.int("KAPG_PG_PORT", 5432),
        "pg_user": env.str("KAPG_PG_USER", "postgres"),
        "pg_password": env.str("KAPG_PG_PWD", "changeme"),
        "pg_db_name": env.str("KAPG_PG_DB_NAME", "metrics"),
        "pg_table_name": env.str("KAPG_PG_TABLE_NAME", "pagemonitor"),
        "pg_conn_timeout": env.float("KAPG_PG_CONN_TIMEOUT", 10.0),
    }
    return DotDict(config)
