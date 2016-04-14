import logging

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.exceptions import AirflowException

_EXECUTOR = configuration.get('core', 'EXECUTOR')

if _EXECUTOR == 'LocalExecutor':
    DEFAULT_EXECUTOR = LocalExecutor()
elif _EXECUTOR == 'CeleryExecutor':
    try:
        from airflow.executors.celery_executor import CeleryExecutor
    except ImportError as e:
        message = (
            "%s, install via 'pip install airflow[celery]'" % e.message)
        raise ImportError(message)
    DEFAULT_EXECUTOR = CeleryExecutor()
elif _EXECUTOR == 'SequentialExecutor':
    DEFAULT_EXECUTOR = SequentialExecutor()
elif _EXECUTOR == 'MesosExecutor':
    from airflow.contrib.executors.mesos_executor import MesosExecutor
    DEFAULT_EXECUTOR = MesosExecutor()
else:
    # Loading plugins
    from airflow.plugins_manager import executors as _executors
    for _executor in _executors:
        globals()[_executor.__name__] = _executor
    if _EXECUTOR in globals():
        DEFAULT_EXECUTOR = globals()[_EXECUTOR]()
    else:
        raise AirflowException("Executor {0} not supported.".format(_EXECUTOR))

logging.info("Using executor " + _EXECUTOR)
