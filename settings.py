import logging
import logging.config
import pathlib as pl

HOST = "ca-pro-rds-prod.cllw6gj7ocf7.us-east-1.rds.amazonaws.com"
DB_NAME = "telegram_data"
DB_USER =  "analytics"
DB_PASS = "acaa301cf8214359b9bb9722b3aa469f"

# TODO: Move all file paths to settings.
dailies_path = pl.Path("info/output/dailies")
merges_path = pl.Path("info/output/channel_data")
final_path = pl.Path("info/output/final")

def configure_logger(name, log_path):
    logging.config.dictConfig({
        'version': 1,
        'formatters': {
            'default': {'format': '%(asctime)s - %(levelname)s - %(message)s', 'datefmt': '%Y-%m-%d %H:%M:%S'}
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'default',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'level': 'DEBUG',
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': log_path,
                'maxBytes': 1024,
                'backupCount': 3
            }
        },
        'loggers': {
            'default': {
                'level': 'DEBUG',
                'handlers': ['console', 'file']
            }
        },
        'disable_existing_loggers': False
    })
    return logging.getLogger(name)
