"""Pre-built loggers to produce structured logs in JSON format."""
import logging
import sys

import structlog
from pythonjsonlogger import jsonlogger


structlog.configure(
    # Assuming logs collected into Elasticsearch
    processors=[
        structlog.processors.JSONRenderer(),
    ]
)

log = structlog.get_logger()

# 3rd party libs are designed to work with logging from python's std lib.
# Trying to pass structlog's logger to kafka producer or backoff handler
# will result in:
# TypeError: _proxy_to_logger() takes from 2 to 3 positional arguments but 7 were given
#
# Let's modify it a bit to produce structured logs:
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(jsonlogger.JsonFormatter())

consumer_log = logging.getLogger("consumer")
consumer_log.addHandler(_handler)
