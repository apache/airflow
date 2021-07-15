import asyncio
import os
from airflow.models import DAG, DagRun
from airflow.utils.curve import get_curve_params, get_task_params, generate_bolt_number, \
    get_craft_type
from typing import Dict
from airflow.api.common.experimental.mark_tasks import modify_task_instance
import datetime as dt
from datetime import timedelta
import pendulum
from airflow.utils.logger import generate_logger
from airflow.api.common.experimental import trigger_dag as trigger
from distutils.util import strtobool
from airflow.hooks.cas_plugin import CasHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_analyze_plugin import TriggerAnalyzeOperator

_logger = generate_logger(__name__)

try:
    ENV_ALWAYS_TRIGGER_ANAY = strtobool(
        os.environ.get('ENV_ALWAYS_TRIGGER_ANAY', 'true'))
except:
    ENV_ALWAYS_TRIGGER_ANAY = True

MAX_ACTIVE_ANALYSIS = 100


def on_dag_fail(context):
    _logger.error("{0} Run Fail".format(repr(context)))


def on_dag_success(context):
    _logger.info("{0} Run Success".format(repr(context)))


dag = DAG(
    dag_id='curve_analyze_dag',
    description=u'上汽拧紧曲线分析',
    schedule_interval=None,
    default_args={
        'owner': 'desoutter',
        'depends_on_past': False,
        'start_date': dt.datetime(2020, 1, 1, tzinfo=pendulum.timezone("Asia/Shanghai")),
        'email': ['support@desoutter.cn'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=2),
        'retry_exponential_backoff': True,
        'on_failure_callback': on_dag_fail,
        'on_success_callback': on_dag_success,
        'on_retry_callback': None,
        'trigger_rule': 'all_success'
    },
    concurrency=MAX_ACTIVE_ANALYSIS,
    max_active_runs=MAX_ACTIVE_ANALYSIS)


trigger_anay_task = TriggerAnalyzeOperator(
            provide_context=True,
            task_id='trigger_anay_task',
            dag=dag,
            priority_weight=9
        )

# test
# https://airflow.apache.org/docs/apache-airflow/1.10.12/executor/debug.html
if __name__ == '__main__':
    from tests.curve_dags.test_trigger import body
    dag.clear(reset_dag_runs=True)
    conf = body.get('conf')
    dag.run(conf=conf)
