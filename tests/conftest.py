import json
import time
from random import randint

import psycopg2
import pytest
from confluent_kafka import Consumer, Producer

from kafka_pg_upload.config import parse_config


@pytest.fixture(scope="module")
def app_conf():
    return parse_config()


@pytest.fixture(scope="module")
def conf_topic():
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"_test_{ts}"


@pytest.fixture(scope="module")
def kafka_producer(app_conf):
    return Producer(
        {
            "bootstrap.servers": app_conf.kafka_broker_list,
        },
    )


@pytest.fixture(scope="module")
def kafka_consumer(app_conf):
    return Consumer(
        {
            "bootstrap.servers": app_conf.kafka_broker_list,
            "group.id": 17,
            "auto.offset.reset": app_conf["consumer_auto.offset.reset"],
        },
    )


@pytest.fixture
def produce_to_kafka(kafka_producer, messages, conf_topic):
    for msg in messages:
        kafka_producer.produce(
            conf_topic,
            key=msg["page_url"],
            value=json.dumps(msg),
        )
    kafka_producer.flush()


@pytest.fixture
def pg_reader(app_conf):
    conn = psycopg2.connect(
        host=app_conf.pg_host,
        port=app_conf.pg_port,
        dbname=app_conf.pg_db_name,
        user=app_conf.pg_user,
        password=app_conf.pg_password,
    )
    return conn


@pytest.fixture()
def messages():
    return [
        # response_time is defined in microseconds
        {
            "ts": "2020-09-03T07:25:26Z",
            "page_url": "https://httpbin.org/status/200",
            "http_code": 200,
            "response_time": randint(5 * 10 ** 5, 9 * 10 ** 5),
        },
        {
            "ts": "2020-09-03T07:26:49Z",
            "page_url": "https://httpbin.org/status/301",
            "http_code": 301,
            "response_time": randint(5 * 10 ** 5, 9 * 10 ** 5),
        },
        {
            "ts": "2020-09-03T07:25:59Z",
            "page_url": "https://httpbin.org/status/404",
            "http_code": 404,
            "response_time": randint(5 * 10 ** 5, 9 * 10 ** 5),
        },
        {
            "ts": "2020-09-03T07:27:33Z",
            "page_url": "https://httpbin.org/status/502",
            "http_code": 502,
            "response_time": randint(5 * 10 ** 5, 9 * 10 ** 5),
        },
    ]
