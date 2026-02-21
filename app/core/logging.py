"""Structured logging configuration for the application."""

import logging
import sys


def configure_logging(log_level: str = "INFO") -> None:
    """Configure the root logger with structured formatting.

    Args:
        log_level: Logging level string (e.g. "INFO", "DEBUG", "ERROR").
    """
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    root_logger.handlers = []
    root_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger instance.

    Args:
        name: Module or component name for the logger.

    Returns:
        A configured Logger instance.
    """
    return logging.getLogger(name)
