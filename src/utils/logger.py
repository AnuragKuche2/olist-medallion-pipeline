# src/utils/logger.py
"""
Structured logging configuration for the pipeline.

Usage:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("Processing table", extra={"table": "orders", "records": 99441})
"""

import logging
import sys


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Create a structured logger with a consistent format.

    Format: 2024-06-15 10:30:45 [INFO] src.bronze.ingest — Processing table: orders
    """
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times (e.g., when module is re-imported)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)

    return logger
