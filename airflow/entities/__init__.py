from . import entity
from . import curve_storage
from . import result_storage
import logging

logger = logging.getLogger('airflow.entities')


def result_hook(result):
    logger.info('result hook not provided')
