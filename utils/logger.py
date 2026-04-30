"""
Structured logger for pipeline modules.

Using a single configured logger across the project keeps log format
consistent and makes it easy to swap to JSON logging or ship to a
collector (e.g. CloudWatch, Datadog) by changing one place.
"""
import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger with consistent formatting.

    Parameters
    ----------
    name : str
        Logger name — typically __name__ of the calling module so the
        log output identifies its source.
    """
    logger = logging.getLogger(name)

    # Avoid attaching the handler more than once if get_logger is called
    # multiple times in the same process.
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger
