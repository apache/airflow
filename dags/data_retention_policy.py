# -*- coding:utf-8 -*-
from airflow.models import DAG, Log, TaskFail, XCom, DagRun
from airflow.settings import TIMEZONE
from airflow.entities.curve_storage import ClsCurveStorage
from plugins.utils import get_curve_args
from airflow.operators.python_operator import PythonOperator
from airflow.models import TaskInstance
from datetime import datetime, timedelta
from dateutil import relativedelta
import os
import logging
from airflow.utils.db import provide_session
from airflow.models.taskreschedule import TaskReschedule
from airflow.utils.state import State
import datetime as dt

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')
if RUNTIME_ENV == 'prod':
    schedule_interval = dt.timedelta(hours=1)
    loggingLevel = logging.INFO
else:
    schedule_interval = '@once'
    loggingLevel = logging.DEBUG

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler())

_logger.setLevel(loggingLevel)

START_DATE = datetime.now(tz=TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0) - relativedelta.relativedelta(
    days=1)

MINIO_ROOT_URL = os.environ.get('MINIO_ROOT_URL', None)

try:
    LIMIT = int(os.environ.get('DATA_VACUUM_LIMIT', '1000'))
except Exception as e:
    _logger.error(e)
    LIMIT = 1000

try:
    DATA_STORAGE_DURATION = int(os.environ.get('DATA_STORAGE_DURATION', '90'))  # 单位为天
except Exception as e:
    _logger.error(e)
    DATA_STORAGE_DURATION = 90
try:
    DATA_STORAGE_MARGIN = int(os.environ.get('DATA_STORAGE_DURATION', '3'))
except Exception as e:
    _logger.error(e)
    DATA_STORAGE_MARGIN = 3

default_args = {
    'owner': "operations",
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'data_retention_policy',
    default_args=default_args,
    schedule_interval=schedule_interval,
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


@provide_session
def get_all_results(start_date=None, end_date=None, limit=1000, session=None):
    from plugins.models.result import ResultModel
    qry = session.query(ResultModel).limit(limit).from_self()
    if start_date:
        qry = qry.filter(ResultModel.update_time >= start_date)
    if end_date:
        qry = qry.filter(ResultModel.update_time <= end_date)
    results = qry.all()
    return results


def get_all_need_delete_results(test_mode, **kwargs):
    delta_time = verify_params(test_mode, **kwargs) - DATA_STORAGE_MARGIN
    delta = relativedelta.relativedelta(days=delta_time)
    end_date = datetime.now(tz=TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0) - delta
    _logger.info("Get End Date Time: {}".format(datetime.isoformat(end_date)))
    results = get_all_results(end_date=end_date, limit=LIMIT)
    return results, end_date


def clear_curve_files(curve_files):
    if not curve_files:
        return
    curve_args = get_curve_args('qcos_minio')
    if MINIO_ROOT_URL:
        curve_args.update({'endpoint': MINIO_ROOT_URL})
    ct = ClsCurveStorage(**curve_args)
    try:
        ct.remove_curves(curve_files=curve_files)
    except Exception as err:
        _logger.error("Remove Curve File Error:", exc_info=err)


def get_all_need_delete_curves(results):
    if not results or not len(results):
        return []
    need_delete_curve_files = []
    for result in results:
        if not result.entity_id:
            continue
        f = "{}.csv".format(result.entity_id)
        need_delete_curve_files.append(f)
    return need_delete_curve_files


@provide_session
def clear_tasks(start_date=None, end_date=None, session=None):
    count = 0
    for model in DagRun, TaskFail, TaskInstance, TaskReschedule, XCom, Log:
        if model == TaskFail or model == XCom or model == TaskReschedule or model == Log:
            qry = session.query(model)
        else:
            qry = session.query(model).filter(model.state == State.SUCCESS)
        if start_date:
            qry = qry.filter(model.execution_date >= start_date)
        if end_date:
            qry = qry.filter(model.execution_date <= end_date)
        count += qry.delete()
    return count


def do_saic_data_retention_policy_task(test_mode, **kwargs):
    results, end_date = get_all_need_delete_results(test_mode, **kwargs)
    need_delete_curve_files = get_all_need_delete_curves(results)
    _logger.info("Try To Remove Object: {}".format(','.join(need_delete_curve_files)))
    ret = clear_tasks(end_date=end_date, limit=LIMIT)
    _logger.info("Clear Task Return: {}".format(ret))
    clear_curve_files(need_delete_curve_files)


auto_vacuum_task = PythonOperator(provide_context=True,
                                  task_id='auto_vacuum_task', dag=dag,
                                  python_callable=do_saic_data_retention_policy_task)
