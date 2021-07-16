from abc import ABC
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.plugins_manager import AirflowPlugin
from typing import Dict
from airflow.utils.curve import get_result_args, get_curve_args
import os
from airflow.models.dagrun import DagRun
from airflow.entities.curve_storage import ClsCurveStorage
from airflow.entities.result_storage import ClsResultStorage

_logger = LoggingMixin().log
SUPPORT_DEVICE_TYPE = ['tightening', 'servo_press']

MINIO_ROOT_URL = os.environ.get('MINIO_ROOT_URL', None)
RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')


class ResultStorageHook(BaseHook, ABC):

    @staticmethod
    def do_push_to_storage(params: Dict):
        _logger.debug('start pushing result & curve...')
        curve_args = get_curve_args()
        if MINIO_ROOT_URL:
            curve_args.update({'endpoint': MINIO_ROOT_URL})
        ct = ClsCurveStorage(**curve_args)
        ct.metadata = params  # 必须在设置curvefile前赋值
        result_args = get_result_args()
        st = ClsResultStorage(**result_args)
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

    @staticmethod
    def is_valid_params(test_mode, **kwargs):
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

    @staticmethod
    def on_curve_receive(test_mode, **kwargs):
        _logger.debug('start storing with kwargs: {0}'.format(kwargs))
        params = ResultStorageHook.is_valid_params(test_mode, **kwargs)
        _logger.info('params verify success')
        _logger.debug('dag_run conf param: {0}'.format(params))  # 从接口中获取的参数
        should_store = True
        if should_store:
            ResultStorageHook.do_push_to_storage(params)
        _logger.info(params)
        return params


# Defining the plugin class
class ResultStoragePlugin(AirflowPlugin):
    name = "result_storage_plugin"
    hooks = [ResultStorageHook]
