# -*- coding:utf-8 -*-

from airflow.models import DAG, DagRun
from airflow.settings import TIMEZONE
from airflow.entities.curve_storage import ClsCurveStorage
from plugins.utils import get_result_args, get_curve_args
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import TaskInstance
from datetime import datetime, timedelta
from dateutil import relativedelta
import os
import logging
import airflow

DAG_ID = 'data_retention_policy'

AUTO_VACUUM_TASK = 'auto_vacuum_task'
START_DATE = datetime.now(tz=TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta.relativedelta(
    days=1)
SCHEDULE_INTERVAL = "@daily"
DAG_OWNER_NAME = "operations"

MINIO_ROOT_URL = os.environ.get('MINIO_ROOT_URL', None)

try:
    LIMIT = int(os.environ.get('DATA_VACUUM_LIMIT', '1000'))
except Exception as e:
    LIMIT = 1000

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

if RUNTIME_ENV == 'prod':
    schedule_interval = SCHEDULE_INTERVAL
    loggingLevel = logging.INFO
else:
    schedule_interval = '@once'
    loggingLevel = logging.DEBUG

try:
    DATA_STORAGE_DURATION = int(os.environ.get('DATA_STORAGE_DURATION', '90'))  # 单位为天
except Exception:
    DATA_STORAGE_DURATION = 90
try:
    DATA_STORAGE_MARGIN = int(os.environ.get('DATA_STORAGE_DURATION', '3'))
except Exception:
    DATA_STORAGE_MARGIN = 3

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler())

_logger.setLevel(loggingLevel)

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE
)


def verify_params(test_mode, **kwargs):
    # dag_run = kwargs.get('dag_run', None)
    params = {}
    if test_mode:
        params = kwargs.get("params", {}).get('conf', {})
    # if isinstance(dag_run, DagRun):
    #     params = getattr(dag_run, 'conf')
    # if isinstance(dag_run, dict):
    #     params = dag_run.get('conf', None)
    # if params is None:
    #     raise Exception(u'参数conf不存在')
    delta_time = params.get('delta_time', DATA_STORAGE_DURATION)
    if isinstance(delta_time, str):
        delta_time = int(delta_time)
    return delta_time


def getAllNeedDelteTaskInstance(test_mode, **kwargs):
    delta_time = verify_params(test_mode, **kwargs) - DATA_STORAGE_MARGIN
    delta = relativedelta.relativedelta(days=delta_time)
    end_date = datetime.now(tz=TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0) - delta
    _logger.info("Get End Date Time: {}".format(datetime.isoformat(end_date)))
    tasks = TaskInstance.get_all_tasks(end_date=end_date, limit=LIMIT)
    return tasks, end_date


def clearCurveFiles(curveFiles):
    if not curveFiles:
        return
    curveArgs = get_curve_args('qcos_minio')
    if MINIO_ROOT_URL:
        curveArgs.update({'endpoint': MINIO_ROOT_URL})
    ct = ClsCurveStorage(**curveArgs)
    try:
        ct.remove_curves(curve_files=curveFiles)
    except Exception as e:
        _logger.error("Remove Curve File Error:", exc_info=e)


def getAllNeedDeleteCurves(tasks):
    if not tasks or not len(tasks):
        return []
    curves = TaskInstance.get_all_curves(tasks)
    return curves


def doSAICDataRetentionPolicyTask(test_mode, **kwargs):
    tasks, end_date = getAllNeedDelteTaskInstance(test_mode, **kwargs)
    needDeleteCurveFiles = getAllNeedDeleteCurves(tasks)
    _logger.info("Try To Remove Object: {}".format(','.join(needDeleteCurveFiles)))
    ret = TaskInstance.clear_tasks(end_date=end_date, limit=LIMIT)
    _logger.info("Clear Task Return: {}".format(ret))
    clearCurveFiles(needDeleteCurveFiles)


auto_vacuum_task = PythonOperator(provide_context=True,
                                  task_id=AUTO_VACUUM_TASK, dag=dag,
                                  python_callable=doSAICDataRetentionPolicyTask)
