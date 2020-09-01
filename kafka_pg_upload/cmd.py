import asyncio
import signal
import sys

import environs

from . import __version__
from .config import parse_config
from .logger import log


async def main():
    # Get Config
    try:
        conf = parse_config()
    except environs.EnvValidationError as err:
        log.error(error=err)
        sys.exit(1)

    # Start
    log.info(
        bin=sys.argv[0],
        version=__version__,
        config=conf,
    )


def run():
    # Handle Ctrl+C
    signal.signal(signal.SIGINT, lambda signal, frame: sys.exit(0))
    asyncio.run(main())
