import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    """Logging Setup
    Args:
        level (str, optional): The logging level. Defaults to "INFO".
    """
    log_level = (level or "INFO").upper()

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )
