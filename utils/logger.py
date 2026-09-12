"""Shared colored-console logger factory used by all pipeline components."""

import logging
import colorlog

def get_logger(name: str) -> logging.Logger:
    """Build a non-propagating logger named `name` with colored INFO-level console output."""
    logger = logging.getLogger(name)
    if logger.handlers:
        # Already configured (e.g. get_logger called twice with the same name) -
        # skip re-adding a handler so log lines aren't duplicated.
        return logger

    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(name)s] %(levelname)s%(reset)s — %(message)s",
        datefmt="%H:%M:%S",
        log_colors={
            "DEBUG":    "cyan",
            "INFO":     "green",
            "WARNING":  "yellow",
            "ERROR":    "red",
            "CRITICAL": "bold_red",
        }
    ))
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger
