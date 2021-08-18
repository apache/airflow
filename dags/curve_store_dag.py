# -*- coding:utf-8 -*-
import datetime as dt
from datetime import timedelta
from airflow.models import DAG
import pendulum
from airflow.operators.python_operator import PythonOperator
from plugins.utils.logger import generate_logger

# MAX_ACTIVE_ANALYSIS = os.environ.get('MAX_ACTIVE_ANALYSIS', 100)
MAX_ACTIVE_ANALYSIS = 8

_logger = generate_logger(__name__)


def on_curve_receive(**kwargs):
    from plugins.result_storage.result_storage_plugin import ResultStorageHook
    params = getattr(kwargs.get('dag_run'), 'conf')
    params = ResultStorageHook.on_curve_receive(params)
    from plugins.trigger_analyze.trigger_analyze_plugin import TriggerAnalyzeHook
    TriggerAnalyzeHook.trigger_analyze(params)


def onCurveAnalyFail(context):
    _logger.error("{0} Run Fail".format(context))


def onCurveAnalySuccess(context):
    _logger.info("{0} Run Success".format(context))


dag = DAG(
    dag_id='curve_anay',
    description=u'上汽拧紧曲线分析',
    schedule_interval=None,
    default_args={
        'owner': 'desoutter',
        'depends_on_past': False,
        'start_date': dt.datetime(2020, 1, 1, tzinfo=pendulum.timezone("Asia/Shanghai")),
        'email': ['support@desoutter.cn'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
        'retry_exponential_backoff': True,
        'on_failure_callback': onCurveAnalyFail,
        'on_success_callback': onCurveAnalySuccess,
        'on_retry_callback': None,
        'trigger_rule': 'all_success'
    },
    concurrency=MAX_ACTIVE_ANALYSIS,
    max_active_runs=MAX_ACTIVE_ANALYSIS
)

store_task = PythonOperator(
    provide_context=True,
    task_id='store_result_curve',
    dag=dag,
    priority_weight=9,
    python_callable=on_curve_receive
)

# test
# https://airflow.apache.org/docs/apache-airflow/1.10.12/executor/debug.html
if __name__ == '__main__':
    from tests.curve_dags.test_trigger import body

    dag.clear(reset_dag_runs=True)
    conf = body.get('conf')
    dag.run(conf=conf)
