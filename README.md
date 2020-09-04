kafka-pg-upload
===============

[![Build Status](https://travis-ci.org/pavdmyt/kafka-pg-upload.svg?branch=master)](https://travis-ci.org/pavdmyt/kafka-pg-upload)

Is a microservice to consume messages from [Kafka](https://kafka.apache.org/)
topic and write them into [PostgreSQL](https://www.postgresql.org/).


# Installation

Run the following commands:

```
$ make build
$ pip install dist/kafka_pg_upload-*-py3-none-any.whl
```


# Configuration

Configuration is provided via environment variables. It contains good defaults
to start experimenting with. It is possible to override defaults either directly
(e.g. `export KAPG_KAFKA_TOPIC=foo`) or via a file:

```
$ cat > kapg.cfg <<EOF
> export KAPG_GROUP_ID=123
> export KAPG_AUTOOFFSETRESET=smallest
>EOF
$
$ source kapg.cfg
```

| Environment variable           | Parameter                | Description                                        | Default                                   |
|--------------------------------|--------------------------|----------------------------------------------------|-------------------------------------------|
| `KAPG_BROKER_LIST`             | `kafka_broker_list`      | Initial list of brokers as host:port (comma delimited) | `"localhost:9092,"`                   |
| `KAPG_KAFKA_TOPIC`             | `kafka_topic`            | Kafka topic name to send monitoring data           | `"pagemonitor_metrics"`                   |
| `KAPG_GROUP_ID`                | `consumer_group.id`      | Client group id string. All clients sharing the same group.id belong to the same group | `42`  |
| `KAPG_AUTOOFFSETRESET`         | `consumer_auto.offset.reset` | Action to take when there is no initial offset in offset store or the desired offset is out of range | `"earliest"` |
| `KAPG_CONSUMER_SLEEP`          | `consumer_sleep_interval` | How long consumer waits when there is no messages to consume (seconds) | `2`                  |
| `KAPG_KAFKA_ENABLE_CERT_AUTH`  | `kafka_enable_cert_auth`  | Enable SSL mode for communication with Kafka      | `False`                                   |
| `KAPG_KAFKA_SSL_CA`            | `kafka_ssl_ca`           | Path to Kafka CA file                              | `"/etc/kapg/ssl/kafka/ca.pem"`            |
| `KAPG_KAFKA_SSL_CERT`          | `kafka_ssl_cert`         | Path to Kafka certificate file                     | `"/etc/kapg/ssl/kafka/service.cert"`      |
| `KAPG_KAFKA_SSL_KEY`           | `kafka_ssl_key`          | Path to Kafka key file                             | `"/etc/kapg/ssl/kafka/service.key"`       |
| `KAPG_PG_HOST`                 | `pg_host`                | PostgreSQL host                                    | `"localhost"`                             |
| `KAPG_PG_PORT`                 | `pg_port`                | PostgreSQL port                                    | `5432`                                    |
| `KAPG_PG_USER`                 | `pg_user`                | PostgreSQL user                                    | `"postgres"`                              |
| `KAPG_PG_PWD`                  | `pg_password`            | PostgreSQL password                                | `"changeme"`                              |
| `KAPG_PG_DB_NAME`              | `pg_db_name`             | PostgreSQL database name to write into             | `"metrics"`                               |
| `KAPG_PG_TABLE_NAME`           | `pg_table_name`          | PostgreSQL table name to write into                | `"pagemonitor"`                           |
| `KAPG_PG_CONN_TIMEOUT`         | `pg_conn_timeout`        | PostgreSQL connection timeout (seconds)            | `10.0`                                    |
| `KAPG_PG_COMMAND_TIMEOUT`      | `pg_command_timeout`     | PostgreSQL default timeout for operations on existing connection (seconds) | `10.0`            |
| `KAPG_PG_ENABLE_SSL`           | `pg_enable_ssl`          | Enable SSL for communication with PostgreSQL       | `False`                                   |
| `KAPG_PG_SSL_CA`               | `pg_ssl_ca`              | Path to PostgreSQL CA file                         | `"/etc/kapg/ssl/postgres/ca.pem"`         |


# Usage

After installation `kafka-pg-upload` executable should be available in your `$PATH`.
Provide a desired config and start the service:

```
$ kafka-pg-upload
{"bin": "/Users/me/.pyenv/versions/playground-py38/bin/kafka-pg-upload", "version": "0.1.0", "config": {"kafka_broker_list": "localhost:9092,", "kafka_topic": "pagemonitor_metrics", "consumer_group.id": "42", "consumer_auto.offset.reset": "earliest", "consumer_sleep_interval": 2.0, "kafka_enable_cert_auth": false, "kafka_ssl_ca": "PosixPath('/etc/kapg/ssl/kafka/ca.pem')", "kafka_ssl_cert": "PosixPath('/etc/kapg/ssl/kafka/service.cert')", "kafka_ssl_key": "PosixPath('/etc/kapg/ssl/kafka/service.key')", "pg_host": "localhost", "pg_port": 5432, "pg_user": "postgres", "pg_password": "changeme", "pg_db_name": "metrics", "pg_table_name": "pagemonitor", "pg_conn_timeout": 10.0, "pg_command_timeout": 10.0, "pg_enable_ssl": false, "pg_ssl_ca": "PosixPath('/etc/kapg/ssl/postgres/ca.pem')"}}
```


# Development

Clone the repository:

```
$ git clone git@github.com:pavdmyt/kafka-pg-upload.git
```

Install dev dependencies:

```
$ poetry install
```

Lint, format, isort code:

```
$ make lint
$ make fmt
$ make isort
```

Run tests:

```
$ make test
```

Build:

```
$ make build
```


# Contributing

1. Fork it!
2. Create your feature branch: `git checkout -b my-new-feature`
3. Commit your changes: `git commit -m 'Add some feature'`
4. Push to the branch: `git push origin my-new-feature`
5. Submit a pull request
6. Make sure tests are passing
