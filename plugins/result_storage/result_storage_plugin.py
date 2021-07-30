from abc import ABC
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.plugins_manager import AirflowPlugin
from typing import Dict
from plugins.utils import get_result_args, get_curve_args
import os
from pprint import pformat
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
        _logger.info('start pushing result & curve...')
        result_args = get_result_args()
        st = ClsResultStorage(**result_args)
        st.metadata = params
        if not st:
            raise Exception('result storage not ready!')
        st.write_result(params)
        _logger.info('pushing result success!!!')
        curve_args = get_curve_args()
        if MINIO_ROOT_URL:
            _logger.debug(f'override OSS URL： {MINIO_ROOT_URL}')
            curve_args.update({'endpoint': MINIO_ROOT_URL})
        ct = ClsCurveStorage(**curve_args)
        params.update({'curveFile': ct.ObjectName})
        ct.metadata = params  # 必须在设置curvefile前赋值
        if not ct:
            raise Exception('curve storage not ready!')
        try:
            _logger.debug(f'write curve params： {pformat(params, indent=4)}')
            ct.write_curve(params)
            _logger.info('pushing curve success')
        except Exception as e:
            _logger.error(f'writing curve error: {repr(e)}')
            raise e

    @staticmethod
    def is_valid_params(params):
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
    def on_curve_receive(params):
        _logger.debug('start storing with params: {0}'.format(repr(params)))
        params = ResultStorageHook.is_valid_params(params)
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
