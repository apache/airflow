import logging
import os

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')


def generate_logger(name, env=RUNTIME_ENV):
    if env == 'prod':
        logging_level = logging.INFO
    else:
        logging_level = logging.DEBUG

    _logger = logging.getLogger(name)
    _logger.addHandler(logging.StreamHandler())
    _logger.setLevel(logging_level)
    return _logger
