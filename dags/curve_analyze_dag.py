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

_logger = generate_logger(__name__)

try:
    ENV_ALWAYS_TRIGGER_ANAY = strtobool(
        os.environ.get('ENV_ALWAYS_TRIGGER_ANAY', 'true'))
except:
    ENV_ALWAYS_TRIGGER_ANAY = True

# MAX_ACTIVE_ANALYSIS = os.environ.get('MAX_ACTIVE_ANALYSIS', 100)
MAX_ACTIVE_ANALYSIS = 100


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


def trigger_push_result_dag(params):
    _logger.info('pushing result to mq...')
    push_result_dat_id = 'publish_result_dag'
    conf = {
        'data': params,
        'data_type': 'tightening_result'
    }
    trigger.trigger_dag(push_result_dat_id, conf=conf,
                        replace_microseconds=False)


def params_should_analyze(params: Dict) -> bool:
    should_analyze = params.get('should_analyze', True)
    return should_analyze


def is_rework_result(params: Dict) -> bool:
    result_body = params.get('result', None)
    if not result_body:
        return False
    job = result_body.get('job', 0)
    batch_count = result_body.get('batch_count', 0)
    if (not job) and (not batch_count):
        return True
    return False


def should_skip_analysis(params: Dict) -> bool:
    if not params_should_analyze(params):
        _logger.info('强制不分析结果，跳过分析...')
        return True
    if ENV_ALWAYS_TRIGGER_ANAY:
        return False
    if is_rework_result(params):
        _logger.info('返修工位曲线，跳过分析...')
        return True
    return False


def do_trigger_anay_task(test_mode, **kwargs):
    params = isValidParams(test_mode, **kwargs)
    task_instance = kwargs.get('task_instance', None)
    new_param = prepare_trigger_params(params, task_instance)
    trigger_push_result_dag(new_param)
    if should_skip_analysis(params):
        return
    cas = CasHook(role='analysis')
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(cas.trigger_analyze(new_param))
    loop.close()
    return result


def on_dag_fail(context):
    _logger.error("{0} Run Fail".format(context))


def on_dag_success(context):
    _logger.info("{0} Run Success".format(context))


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

trigger_anay_task = PythonOperator(
    provide_context=True,
    task_id='trigger_anay_task',
    dag=dag,
    priority_weight=9,
    python_callable=do_trigger_anay_task
)
