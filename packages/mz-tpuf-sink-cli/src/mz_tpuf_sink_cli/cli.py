"""Command-line entry point: load settings, install signal handlers, run."""

from __future__ import annotations

import logging
import signal
import threading

import click
from mz_tpuf_sink import run_sink

from .settings import Settings

logger = logging.getLogger(__name__)


@click.command()
@click.option("--log-level", default="INFO", show_default=True)
@click.version_option(package_name="mz-tpuf-sink-cli")
def main(log_level: str) -> None:
    """Atomically sink a Materialize Kafka topic into turbopuffer.

    Configuration comes from MZ_TPUF_* environment variables or a .env file;
    see the README for the full reference.
    """
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    config = Settings().to_config()

    # signal handling belongs to the process, not the library
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    run_sink(config, stop=stop)


if __name__ == "__main__":
    main()
