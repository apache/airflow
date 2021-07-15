# -*- coding:utf-8 -*-
import os
import datetime as dt
from datetime import timedelta
from airflow.models import DAG, DagRun
from airflow.utils.curve import get_curve_params, get_task_params, generate_bolt_number, \
    get_craft_type
import pendulum
from airflow.operators.python_operator import PythonOperator
from typing import Dict, Any
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from airflow.api.common.experimental.mark_tasks import modify_task_instance
from airflow.entities.curve_storage import ClsCurveStorage
from airflow.entities.result_storage import ClsResultStorage
from airflow.utils.logger import generate_logger
from airflow.api.common.experimental import trigger_dag as trigger
from airflow.models.tightening_controller import TighteningController
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

_logger = generate_logger(__name__)


def do_push_to_sotrage(params: Dict):
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
    should_analyze = params.get('should_analyze', True)
    if not should_analyze:
        _logger.info('接收到不分析指令，跳过分析...')
        return True
    if ENV_ALWAYS_TRIGGER_ANAY:
        return False
    if is_rework_result(params):
        _logger.info('返修工位曲线，跳过分析...')
        return True
    return False


def on_curve_receive(test_mode, **kwargs):
    _logger.debug('start storing with kwargs: {0}'.format(kwargs))
    params = isValidParams(test_mode, **kwargs)
    _logger.info('params verify success')
    _logger.debug('dag_run conf param: {0}'.format(params))  # 从接口中获取的参数
    should_store = True
    should_analysis = not should_skip_analysis(params)
    if should_store:
        do_push_to_sotrage(params)
        # trigger_push_result_dag(params)
    if should_analysis:
        trigger.trigger_dag('curve_analyze_dag', conf=params, replace_microseconds=False)



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
    concurrency=100,
    max_active_runs=MAX_ACTIVE_ANALYSIS
)

store_task = PythonOperator(
    provide_context=True,
    task_id='store_result_curve',
    dag=dag,
    priority_weight=2,
    python_callable=on_curve_receive
)
