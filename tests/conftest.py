import time
from random import randint

import psycopg2
import pytest
from confluent_kafka import Producer

from kafka_pg_upload.config import parse_config


@pytest.fixture
def app_conf():
    return parse_config()


@pytest.fixture(scope="session")
def conf_broker_list():
    return "localhost:9092,"


@pytest.fixture(scope="session")
def conf_topic():
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"_test_{ts}"


@pytest.fixture(scope="session")
def kafka_producer(conf_broker_list):
    return Producer(
        {
            "bootstrap.servers": conf_broker_list,
        },
    )


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
