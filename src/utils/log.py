import sys
import logging


def init_logger(level=logging.INFO) -> logging.Logger:
    """
    Create the logging layer.
    """
    logger = logging.getLogger("clarityai")

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
    return logger
