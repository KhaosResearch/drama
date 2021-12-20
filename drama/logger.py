import logging.config
from typing import Optional

DEFAULT_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "basic": {
            "format": "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }
    },
    "handlers": {
        "console": {
            "formatter": "basic",
            "class": "logging.StreamHandler",
        },
        "rotate_file": {
            "formatter": "basic",
            "class": "logging.handlers.RotatingFileHandler",
            "filename": "drama.log",
            "encoding": "utf8",
            "maxBytes": 100000,
            "backupCount": 1,
        },
    },
    "loggers": {
        "": {
            "level": "INFO",
            "handlers": ["console"],
        },
        "drama": {
            "level": "DEBUG",
            "handlers": ["rotate_file"],
        },
    },
}


def configure_logging(config: Optional[dict] = None):
    logging.config.dictConfig(config or DEFAULT_LOGGING_CONFIG)


def get_logger(module, name: str = None):
    logger_name = module
    if name is not None:
        logger_name += "." + name
    return logging.getLogger(logger_name)
