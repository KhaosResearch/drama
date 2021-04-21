import logging.config

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
            "level": "WARN",
            "handlers": ["rotate_file"],
        },
        "drama": {
            "level": "DEBUG",
            "handlers": ["console"],
        },
    },
}


def configure_logging():
    logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)


def get_logger(module, name: str = None):
    logger_name = module
    if name is not None:
        logger_name += "." + name
    return logging.getLogger(logger_name)
