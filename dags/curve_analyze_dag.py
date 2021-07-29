import os
from airflow.models import DAG
import datetime as dt
from datetime import timedelta
import pendulum
from airflow.utils.logger import generate_logger
from distutils.util import strtobool
from airflow.operators.trigger_analyze_plugin import TriggerAnalyzeOperator

_logger = generate_logger(__name__)

try:
    ENV_ALWAYS_TRIGGER_ANAY = strtobool(
        os.environ.get('ENV_ALWAYS_TRIGGER_ANAY', 'true'))
except:
    ENV_ALWAYS_TRIGGER_ANAY = True

MAX_ACTIVE_ANALYSIS = 100


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
    from airflow.hooks.result_storage_plugin import ResultStorageHook

    params = ResultStorageHook.on_curve_receive(body.get('conf'))
    dag.clear(reset_dag_runs=True)
    dag.run(conf=params)
