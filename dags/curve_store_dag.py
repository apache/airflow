# -*- coding:utf-8 -*-
import asyncio
import os
import datetime as dt
from datetime import timedelta
from airflow.models import DAG, DagRun
from airflow.utils.curve import get_cas_analysis_base_url, get_curve_params, get_task_params, generate_bolt_number, \
    get_craft_type
import pendulum
from airflow.operators.python_operator import PythonOperator
from typing import Dict, Any
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from aiohttp import ClientTimeout
from aiohttp_retry import RetryClient
from airflow.api.common.experimental.mark_tasks import modify_task_instance
import json
from airflow.entities.curve_storage import ClsCurveStorage
from airflow.entities.result_storage import ClsResultStorage
from airflow.utils.logger import generate_logger
from airflow.api.common.experimental import trigger_dag as trigger
from airflow.models.tightening_controller import TighteningController
from airflow.utils.db import get_connection
from airflow.utils.curve import get_result_args, get_curve_args
from distutils.util import strtobool

MINIO_ROOT_URL = os.environ.get('MINIO_ROOT_URL', None)

try:
    ENV_ALWAYS_TRIGGER_ANAY = strtobool(
        os.environ.get('ENV_ALWAYS_TRIGGER_ANAY', 'true'))
except:
    ENV_ALWAYS_TRIGGER_ANAY = True

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')
# MAX_ACTIVE_ANALYSIS = os.environ.get('MAX_ACTIVE_ANALYSIS', 100)
MAX_ACTIVE_ANALYSIS = 100
STORE_TASK = 'store_result_curve'
TRIGGER_ANAY_TASK = 'trigger_anay_task'
DAG_ID = 'curve_anay'

if RUNTIME_ENV == 'prod':
    schedule_interval = None
    write_options = SYNCHRONOUS
else:
    schedule_interval = None
    write_options = ASYNCHRONOUS

_logger = generate_logger(__name__)


def onCurveAnalyFail(context):
    _logger.error("{0} Run Fail".format(context))


def onCurveAnalySuccess(context):
    _logger.info("{0} Run Success".format(context))


local_tz = pendulum.timezone("Asia/Shanghai")

desoutter_default_args = {
    'owner': 'desoutter',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 1, 1, tzinfo=local_tz),
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
}


def doPush2Storage(params: Dict):
    _logger.debug('start pushing result & curve...')
    curveArgs = get_curve_args()
    if MINIO_ROOT_URL:
        curveArgs.update({'endpoint': MINIO_ROOT_URL})
    ct = ClsCurveStorage(**curveArgs)
    ct.metadata = params  # 必须在设置curvefile前赋值
    resultArgs = get_result_args()
    st = ClsResultStorage(**resultArgs)
    st.metadata = params
    params.update({'curveFile': ct.ObjectName})
    if not st:
        raise Exception('result storage not ready!')
    _logger.debug('pushing result...')
    st.write_result(params)
    if not ct:
        raise Exception('curve storage not ready!')
    _logger.debug('pushing curve...')
    try:
        ct.write_curve(params)
    except Exception as e:
        _logger.info('writing curve error')
        _logger.info(repr(e))
        _logger.info(repr(params))
        raise e


SUPPORT_DEVICE_TYPE = ['tightening', 'servo_press']


def isValidParams(test_mode, **kwargs):
    dag_run = kwargs.get('dag_run', None)
    params = None
    if test_mode:
        dag_run = kwargs.get('params', None)
    if not dag_run:
        raise Exception(u'参数dag_run不存在')
    if isinstance(dag_run, DagRun):
        params = getattr(dag_run, 'conf')
    if isinstance(dag_run, dict):
        params = dag_run.get('conf', None)
    if not params:
        raise Exception(u'参数params不存在')
    result = params.get('result', None)
    if not result:
        raise Exception(u'参数params中result不存在')
    device_type = result.get('device_type', 'tightening')
    if not device_type:  # 如果是空值,设定为默认值
        device_type = 'tightening'
    if device_type not in SUPPORT_DEVICE_TYPE:
        raise Exception(u'参数result中设备类型: {}不支持'.format(device_type))
    result.update({'device_type': device_type})
    return params


def trigger_push_result_dag(params):
    _logger.info('pushing result to mq...')
    push_result_dat_id = 'publish_result_dag'
    conf = {
        'data': params,
        'data_type': 'tightening_result'
    }
    trigger.trigger_dag(push_result_dat_id, conf=conf,
                        replace_microseconds=False)


def doStoreTask(test_mode, **kwargs):
    _logger.debug('start storing with kwargs: {0}'.format(kwargs))
    params = isValidParams(test_mode, **kwargs)
    _logger.info('params verify success')
    _logger.debug('dag_run conf param: {0}'.format(params))  # 从接口中获取的参数
    _logger.info('pushing result to storage...')
    doPush2Storage(params)
    _logger.info('push to storage success')


def get_line_code_by_controller_name(controller_name):
    controller_data = TighteningController.find_controller(controller_name)
    if not controller_data:
        raise Exception('未找到控制器数据: {}'.format(controller_name))
    controller = '{}@{}/{}'.format(controller_data.get('controller_name'),
                                   controller_data.get('work_center_code'),
                                   controller_data.get('work_center_name'))
    return controller_data.get('line_code', None), controller


def prepare_trigger_params(params: Dict, task_instance):
    result_body = params.get('result', None)
    vin = result_body.get('vin', None)
    params_should_anaylze = params.get('should_analyze', True)
    device_type = result_body.get('device_type', 'tightening')
    entity_id = params.get('entity_id', None)
    factory_code = params.get('factory_code', None)
    new_param = params.copy()
    # 螺栓编码生成规则：控制器名称-job号-批次号
    controller_name = result_body.get('controller_name', None)
    job = result_body.get('job', None)
    batch_count = result_body.get('batch_count', None)
    pset = result_body.get('pset', None)
    bolt_number = generate_bolt_number(controller_name, job, batch_count, pset)
    line_code, full_name = get_line_code_by_controller_name(controller_name)
    ti_type = get_task_instance_type(new_param)
    _logger.info("type: {}, entity_id: {}, line_code: {}, bolt_number: {}, factory_code: {}"
                 .format(ti_type, entity_id, line_code, bolt_number, factory_code))
    measure_result = result_body.get('measure_result', None)

    def modifier(ti):
        ti.entity_id = entity_id
        ti.line_code = line_code
        ti.factory_code = factory_code
        ti.controller_name = full_name
        ti.bolt_number = bolt_number
        ti.measure_result = measure_result
        ti.car_code = vin
        if hasattr(ti, 'type'):
            ti.type = ti_type
        if hasattr(ti, 'device_type'):
            ti.device_type = device_type
        if hasattr(ti, 'should_analyze'):
            ti.should_analyze = params_should_anaylze
        _logger.debug("vin: {}".format(ti.car_code))

    modify_task_instance(
        task_instance.dag_id,
        task_instance.task_id,
        task_instance.execution_date,
        modifier=modifier
    )

    try:
        craft_type = get_craft_type(bolt_number)
        _logger.info("craft_type: {}".format(craft_type))
    except Exception as e:
        _logger.error(e)
        craft_type = 1
        _logger.info('使用默认工艺类型：{}'.format(craft_type))

    def store_craft_type(ti):
        ti.craft_type = craft_type

    modify_task_instance(
        task_instance.dag_id,
        task_instance.task_id,
        task_instance.execution_date,
        modifier=store_craft_type
    )

    try:
        curve_params = get_curve_params(bolt_number)
    except Exception as e:
        _logger.error(e)
        curve_params = {}
        _logger.info('无法获取曲线参数（{}/{}）'.format(bolt_number, craft_type))

    task_params = get_task_params(task_instance, entity_id)
    new_param.update(curve_params)
    new_param.update(task_params)
    new_param.update({'craft_type': craft_type})
    return new_param


def get_trigger_training_endpoint():
    return '{}/{}'.format(get_cas_analysis_base_url(), 'cas/analysis')


def is_rework_result(params: Dict) -> bool:
    result_body = params.get('result', None)
    if not result_body:
        return False
    job = result_body.get('job', 0)
    batch_count = result_body.get('batch_count', 0)
    if (not job) and (not batch_count):
        return True
    return False


def params_should_anaylze(params: Dict) -> bool:
    should_analyze = params.get('should_analyze', True)
    return should_analyze


def get_task_instance_type(params: Dict) -> str:
    ret = 'normal'
    if is_rework_result(params):
        return 'rework'
    return ret


def should_skip_analysis(params: Dict) -> bool:
    if not params_should_anaylze(params):
        _logger.info('强制不分析结果，跳过分析...')
        return True
    if ENV_ALWAYS_TRIGGER_ANAY:
        return False
    if is_rework_result(params):
        _logger.info('返修工位曲线，跳过分析...')
        return True
    return False


async def push_result_to_training_server(new_params):
    headers = {
        'Accept': 'application/json',
        'Content-type': 'application/json'
    }
    data = {
        'conf': new_params
    }
    try:
        url = get_trigger_training_endpoint()
        if not new_params:
            raise Exception(u'数据为空')
        if not new_params.get('curve_param', None):
            raise Exception(u'未提供曲线参数')
        _logger.info('参数验证通过，触发分析...')
        async with RetryClient(timeout=ClientTimeout(30)) as client:
            async with client.post(headers=headers, url=url, retry_attempts=5, json=data) as r:
                r.raise_for_status()
                resp = await r.read()
                _logger.debug("trigger training: {}, resp: {}".format(
                    json.dumps(data), resp))
                return resp
    except BaseException as e:
        _logger.error(
            "push_result_to_training_server except: {}".format(repr(e)))
        raise e


def doTriggerAnayTask(test_mode, **kwargs):
    params = isValidParams(test_mode, **kwargs)
    task_instance = kwargs.get('task_instance', None)
    new_param = prepare_trigger_params(params, task_instance)
    trigger_push_result_dag(new_param)
    if should_skip_analysis(params):
        return
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(push_result_to_training_server(new_param))
    loop.close()
    return result


dag = DAG(
    dag_id=DAG_ID,
    description=u'上汽拧紧曲线分析',
    schedule_interval=schedule_interval,
    default_args=desoutter_default_args,
    concurrency=100,
    max_active_runs=MAX_ACTIVE_ANALYSIS)

store_task = PythonOperator(provide_context=True,
                            task_id=STORE_TASK, dag=dag, priority_weight=2,
                            python_callable=doStoreTask)

trigger_anay_task = PythonOperator(provide_context=True,
                                   task_id=TRIGGER_ANAY_TASK, dag=dag,
                                   python_callable=doTriggerAnayTask)

# [store_task, trigger_anay_task]
