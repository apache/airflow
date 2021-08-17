from plugins.utils.logger import generate_logger
import os
from airflow.models import DAG
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from datetime import timedelta
from plugins.publish_result.publish_result_plugin import PublishResultOperator
import datetime as dt
import pendulum

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

if RUNTIME_ENV == 'prod':
    schedule_interval = None
    write_options = SYNCHRONOUS
else:
    schedule_interval = None
    write_options = ASYNCHRONOUS

_logger = generate_logger(__name__)


def on_dag_fail(context):
    _logger.error("{0} Run Fail".format(context))


def on_dag_success(context):
    _logger.info("{0} Run Success".format(context))


dag = DAG(
    dag_id='publish_result_dag',
    description=u'上汽拧紧曲线分析结果推送',
    schedule_interval=schedule_interval,
    default_args={
        'owner': 'desoutter',
        'depends_on_past': False,
        'start_date': dt.datetime(2020, 1, 1, tzinfo=pendulum.timezone("Asia/Shanghai")),
        'email': ['support@desoutter.cn'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 4,
        'retry_delay': timedelta(minutes=2),
        'on_failure_callback': on_dag_fail,
        'on_success_callback': on_dag_success,
        'on_retry_callback': None,
        'trigger_rule': 'all_success'
    },
    max_active_runs=64,
    concurrency=64)


publish_task = PublishResultOperator(
    task_id='publish_result_task',
    dag=dag
)
