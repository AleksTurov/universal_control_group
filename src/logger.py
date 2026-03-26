import logging
import sys
from pathlib import Path

from src.config import path_config


def init_logger() -> logging.Logger:
    """Создает общий логгер проекта."""
    logger = logging.getLogger("my_logger")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    log_format = "%(asctime)s | %(name)s - %(levelname)s - %(message)s | %(pathname)s:%(lineno)d"
    formatter = logging.Formatter(log_format)

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    log_file_path = Path(path_config.BASEDIR) / "logs" / "system.log"
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


logger = init_logger()