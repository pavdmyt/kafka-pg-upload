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
        "kafka_broker_list": env.str("PAGEMON_BROKER_LIST", "localhost:9092,"),
        "kafka_topic": env.str("PAGEMON_KAFKA_TOPIC", "pagemonitor_metrics"),
        # How many times to retry sending a failing Message.
        "producer_retries": env.int("PAGEMON_PRODUCER_RETRIES", 3),
    }
    return DotDict(config)
