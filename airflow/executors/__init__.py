import logging

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor
from airflow.exceptions import AirflowException

_EXECUTOR = configuration.get_executor_type()

if _EXECUTOR == 'localexecutor':
    # LocalExecutor
    from airflow.executors.local_executor import LocalExecutor
    DEFAULT_EXECUTOR = LocalExecutor()
elif _EXECUTOR == 'celeryexecutor':
    # CeleryExecutor
    try:
        from airflow.executors.celery_executor import CeleryExecutor
    except ImportError as e:
        message = (
            "%s, install via 'pip install airflow[celery]'" % e.message)
        raise ImportError(message)
    DEFAULT_EXECUTOR = CeleryExecutor()
elif _EXECUTOR == 'sequentialexecutor':
    # SequentialExecutor
    from airflow.executors.sequential_executor import SequentialExecutor
    DEFAULT_EXECUTOR = SequentialExecutor()
elif _EXECUTOR == 'mesosexecutor':
    # MesosExecutor
    from airflow.contrib.executors.mesos_executor import MesosExecutor
    DEFAULT_EXECUTOR = MesosExecutor()
else:
    # Loading plugins
    from airflow.plugins_manager import executors as _executors
    for _executor in _executors:
        # case insensitive executor names reference to
        # the executor classes
        executor_type = _executor.__name__.lower()
        globals()[executor_type] = _executor
    if _EXECUTOR in globals():
        DEFAULT_EXECUTOR = globals()[_EXECUTOR]()
    else:
        raise AirflowException("Executor {0} not supported.".format(_EXECUTOR))

logging.info("Using executor " + _EXECUTOR)
