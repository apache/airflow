# -*- coding:utf-8 -*-

import os
import datetime as dt
from datetime import timedelta
import pendulum
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from typing import Dict
import logging
from airflow.utils.curve import get_cas_training_base_url, get_cas_analysis_base_url, get_curve_template_name
from airflow.models import Variable
from aiohttp import ClientTimeout
from aiohttp_retry import RetryClient
import asyncio
from airflow.entities.redis import ClsRedisConnection
from airflow.utils.curve import parse_template_name

CURVE_MODE_MAP = {
    'OK': 0,
    'NOK': 1,
}

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

SCHEDULE_INTERVAL = os.environ.get('SCHEDULE_INTERVAL', '@daily')

STORE_TASK = 'curve_tmpls_2_redis'

DAG_ID = 'load_all_curve_tmpls'

IS_DEBUG = RUNTIME_ENV != 'prod'

if RUNTIME_ENV == 'prod':
    schedule_interval = SCHEDULE_INTERVAL
    loggingLevel = logging.INFO
else:
    schedule_interval = '@once'
    loggingLevel = logging.DEBUG

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.StreamHandler())

_logger.setLevel(loggingLevel)


def onLoadCurveTmplsFail(context):
    _logger.error("{0} Run Fail".format(context))


def onLoadCurveTmplsSuccess(context):
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
    'on_failure_callback': onLoadCurveTmplsFail,
    'on_success_callback': onLoadCurveTmplsSuccess,
    'on_retry_callback': None,
    'trigger_rule': 'all_success'
}


def getCurveMode(curveMode: str) -> int:
    scurveMode = curveMode.upper()
    if scurveMode not in CURVE_MODE_MAP.keys():
        return CURVE_MODE_MAP.get('OK')
    else:
        return CURVE_MODE_MAP.get(scurveMode)


redis = None


def remove_outdated_templates():
    global redis
    if not redis:
        redis = ClsRedisConnection()
    keys = redis.read_template_keys()
    _logger.info('当前已加载{}个模板，检测是否过期...'.format(len(keys)))
    templates_to_remove = []
    for key in keys:
        name = parse_template_name(key.decode())
        try:
            k, v = Variable.get_fuzzy_active(name, deserialize_json=True)
            _logger.info('模板{}在使用中'.format(k))
        except:
            _logger.info('模板{}已过期，将从redis中删除'.format(name))
            templates_to_remove.append(key)
    _logger.info('共删除{}个过期模板'.format(len(templates_to_remove)))
    if len(templates_to_remove) > 0:
        redis.remove_templates(templates_to_remove)


def doLoadTmpls2Redis(template_names=None):
    global redis
    if not redis:
        redis = ClsRedisConnection()
    if not redis:
        raise Exception('Redis not init')
    templates: Dict = get_templates_from_variables(template_names)
    redis.store_templates(templates)
    _logger.info('{}个模板加载完成'.format(len(templates.keys())))


def get_templates_from_variables(template_names=None) -> Dict:
    # 如果没有指定template_names，加载所有模板
    if not template_names or len(template_names) == 0:
        return Variable.get_all_active_curve_tmpls()
    # 加载指定模板
    templates = {}
    for t in template_names:
        key, val = Variable.get_fuzzy_active(t, deserialize_json=True, default_var=None)
        template_name = get_curve_template_name(key)
        templates[template_name] = val
    return templates


def get_cas_templates_endpoint(baseUrl):
    return '{}/{}'.format(baseUrl, 'cas/templates')


async def training_server_update_templates():
    headers = {
        'Accept': 'application/json',
        'Content-type': 'application/json'
    }
    analysisUrl = get_cas_templates_endpoint(get_cas_analysis_base_url())
    trainingUrl = get_cas_templates_endpoint(get_cas_training_base_url())
    for url in list(dict.fromkeys([analysisUrl, trainingUrl])):  # remove duplicate
        async with RetryClient(timeout=ClientTimeout(300)) as client:
            async with client.post(headers=headers, url=url, retry_attempts=5) as r:
                r.raise_for_status()
                resp = await r.read()
                _logger.debug("cas/templates called, url: {} resp: {}".format(url, resp))


def doLoadCurveTmplsTask(**kwargs):
    _logger.debug('kwargs: {0}'.format(kwargs))
    template_names = kwargs.get('template_names', None)
    remove_outdated_templates()
    _logger.debug("Loading Curve Templates to Redis...")
    doLoadTmpls2Redis(template_names)
    _logger.debug("Load Curve Templates to Redis Success!")
    _logger.debug("informing training server to update templates...")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(training_server_update_templates())
    loop.close()
    _logger.debug("task finished")


dag = DAG(
    dag_id=DAG_ID,
    description=u'上汽拧紧曲线分析加载模板曲线',
    schedule_interval=schedule_interval,
    default_args=desoutter_default_args,
    max_active_runs=100,
    catchup=False
)

load_curve_tmpl_task = PythonOperator(provide_context=True,
                                      task_id=STORE_TASK, dag=dag,
                                      python_callable=doLoadCurveTmplsTask)
