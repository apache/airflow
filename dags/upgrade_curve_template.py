# -*- coding:utf-8 -*-

import os
import json
import uuid
import pprint
import datetime as dt
from datetime import timedelta
import pendulum
import logging
import pika
from typing import Optional
from airflow.models import DAG
from plugins.models.curve_template import CurveTemplateModel
from typing import Dict
from airflow.operators.python_operator import PythonOperator
from airflow.entities.redis import ClsRedisConnection, gen_template_key
from airflow.entities.result_mq import ClsResultMQ
from plugins.utils import parse_template_name

CURVE_TEMPLATE_UPGRADE_TASK = 'curve_template_upgrade'

CURVE_TEMPLATE_KEY_PREFIX = os.environ.get(
    "CURVE_TEMPLATE_KEY_PREFIX", "templates")

DAG_ID = 'curve_template_upgrade'

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

if RUNTIME_ENV == 'prod':
    schedule_interval = '@once'
    loggingLevel = logging.INFO
else:
    schedule_interval = '@once'
    loggingLevel = logging.DEBUG

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler())

_logger.setLevel(loggingLevel)


def onUpgradeCurveTmplFail(context):
    _logger.error("{0} Run Fail".format(context))


def onUpgradeCurveTmplSuccess(context):
    _logger.info("{0} Run Success".format(context))


local_tz = pendulum.timezone("Asia/Shanghai")

desoutter_default_args = {
    'owner': 'desoutter',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 1, 1, tzinfo=local_tz),
    'email': ['support@desoutter.cn'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': onUpgradeCurveTmplFail,
    'on_success_callback': onUpgradeCurveTmplSuccess,
    'on_retry_callback': None,
    'trigger_rule': 'all_success'
}

mq_connection: Optional[ClsResultMQ] = None
redis_connection: Optional[ClsRedisConnection] = None


def template_upgrade_handler(ch, method: pika.spec.Basic.Deliver, properties: pika.spec.BasicProperties, body: bytes):
    try:
        if not body:
            return
        data = body
        channel = method.routing_key or ''
        if not data or not channel:
            return
        if isinstance(channel, bytes):
            channel = channel.decode('utf-8')
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        template: Dict = json.loads(data)
        _logger.debug("Recv Template Data, key:{}, data: {}".format(channel, pprint.pformat(template, indent=4)))
        template_name = parse_template_name(channel)
    except Exception as e:
        _logger.error("template_upgrade_handler error: {}".format(repr(e)))
        raise e
    try:
        key, val = CurveTemplateModel.get_fuzzy_active(key=template_name, deserialize_json=True)
        _logger.debug("Get Template Var: {}".format(key))
        windows = val.get('curve_param').get('windows', None) if val.get('curve_param', False) else None
        if windows:
            template.update({'windows': windows})
        template.update({'version': val.get('version', 0) + 1})
        CurveTemplateModel.set(key=key, value=template, serialize_json=True)  # 此业务场景下 params不会变化故覆盖现有的variable

    except KeyError as e:
        # 没有这个key 重新创建这个key
        key = "{}@@{}".format(template_name, str(uuid.uuid4()))
        CurveTemplateModel.set(key=key, value=template, serialize_json=True)
    except Exception as e:
        _logger.error("template_upgrade_handler error: {}".format(repr(e)))
    try:
        global redis_connection
        if redis_connection is None:
            raise Exception('redis not connected')
        redis_connection.store_templates({
            template_name: json.dumps(template)
        })
    except Exception as e:
        _logger.error('store curve template to redis error: {}'.format(repr(e)))


def curve_template_upgrade_task(**kwargs):
    global mq_connection, redis_connection
    _logger.debug("{} context: {}".format('curve_template_upgrade_task', kwargs))
    mq_connection = ClsResultMQ(**ClsResultMQ.get_result_mq_args(key='qcos_rabbitmq'))
    redis_connection = ClsRedisConnection()
    queue = os.environ.get('MQ_TEMPLATE_QUEUE', 'qcos_templates_airflow')
    exchange = os.environ.get('MQ_TEMPLATE_EXCHANGE', 'qcos_templates')
    routing_key = gen_template_key('*')

    mq_connection.doSubscribe(queue=queue, message_handler=template_upgrade_handler, exchange=exchange,
                              exchange_type='fanout', routing_key=routing_key)
    mq_connection.run(queue=queue)
    mq_connection.doUnsubscribe(queue)


dag = DAG(
    dag_id=DAG_ID,
    description=u'上汽拧紧曲线分析-曲线模板更新',
    start_date=dt.datetime(2020, 1, 1, tzinfo=local_tz),
    max_active_runs=1,
    schedule_interval=timedelta(seconds=1),
    catchup=True
)

upgrade_curve_template_task = PythonOperator(dag=dag, provide_context=True, task_id=CURVE_TEMPLATE_UPGRADE_TASK,
                                             python_callable=curve_template_upgrade_task)
