# -*- coding:utf-8 -*-
import datetime as dt
from datetime import timedelta
from airflow.models import DAG
import pendulum
from airflow.operators.python_operator import PythonOperator
from plugins.entities.kafka_consumer import ClsKafkaConsumer
from plugins.utils.logger import generate_logger
from airflow.utils.db import get_connection

_logger = generate_logger(__name__)

retry_delay = 10

dag = DAG(
    dag_id='curve_kafka_consume',
    description=u'消费Kafka拧紧结果',
    schedule_interval=timedelta(seconds=1),
    default_args={
        'owner': 'desoutter',
        'depends_on_past': False,
        'start_date': dt.datetime(2020, 1, 1, tzinfo=pendulum.timezone("Asia/Shanghai")),
        'email': ['support@desoutter.cn'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'trigger_rule': 'all_success'
    },
    concurrency=1,
    max_active_runs=1
)


def curve_data_handler(msg):
    try:
        _logger.debug(f'收到kafka数据包:{msg}')
        data = {
            'params': {
                'conf': msg
            }
        }
        from airflow.hooks.result_storage_plugin import ResultStorageHook
        params = ResultStorageHook.on_curve_receive(True, data)
        from airflow.hooks.trigger_analyze_plugin import TriggerAnalyzeHook
        TriggerAnalyzeHook.trigger_analyze(params)
    except Exception as e:
        _logger.error(repr(e))


def get_kafka_config():
    key = 'qcos_kafka'
    conn = get_connection('qcos_kafka')
    if conn is None:
        raise Exception('缺少kafka配置：{}'.format(key))
    data = {
        "user": conn.login,
        "password": conn.get_password(),
        "topic": conn.schema,
        "bootstrap_servers": conn.host.split(',')
    }
    data.update(conn.extra_dejson)
    return data


def watch_kafka_curve(*args, **kwargs):
    consumer = ClsKafkaConsumer(
        **get_kafka_config(),
        handler=curve_data_handler
    )
    while True:
        try:
            consumer.read()
        except Exception as e:
            _logger.error(repr(e))
            # _logger.info('retry in {} seconds'.format(retry_delay))
            # time.sleep(retry_delay)
            continue


kafka_store_task = PythonOperator(
    provide_context=True,
    task_id='kafka_store_task',
    dag=dag,
    priority_weight=9,
    python_callable=watch_kafka_curve
)
