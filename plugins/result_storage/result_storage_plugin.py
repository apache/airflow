from abc import ABC
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.plugins_manager import AirflowPlugin
from plugins.utils import get_curve_args
import os
from pprint import pformat
from airflow.models.dagrun import DagRun
from airflow.entities.curve_storage import ClsCurveStorage
from airflow.entities.result_storage import ClsResultStorage
from plugins.utils import generate_bolt_number
from airflow.models.tightening_controller import TighteningController
from plugins.utils import get_craft_type

_logger = LoggingMixin().log
SUPPORT_DEVICE_TYPE = ['tightening', 'servo_press']

MINIO_ROOT_URL = os.environ.get('MINIO_ROOT_URL', None)
RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

class ResultStorageHook(BaseHook, ABC):

    @staticmethod
    def get_line_code_by_controller_name(controller_name):
        controller_data = TighteningController.find_controller(controller_name)
        if not controller_data:
            raise Exception('未找到控制器数据: {}'.format(controller_name))
        controller = '{}@{}/{}'.format(controller_data.get('controller_name'),
                                       controller_data.get('work_center_code'),
                                       controller_data.get('work_center_name'))
        return controller_data.get('line_code', None), controller

    @staticmethod
    def save_result(entity_id, result, **extra):
        _logger.info('start pushing result...')
        st = ClsResultStorage()
        st.metadata = {
            'entity_id': entity_id
        }
        if not st:
            raise Exception('result storage not ready!')
        _logger.debug('pushing result...')
        result_to_write = extra.copy()

        result_to_write.update(result)
        st.write_result(result_to_write)

    @staticmethod
    def save_curve(params):
        _logger.info('start pushing curve...')
        curve_args = get_curve_args()
        if MINIO_ROOT_URL:
            _logger.debug(f'override OSS URL： {MINIO_ROOT_URL}')
            curve_args.update({'endpoint': MINIO_ROOT_URL})
        ct = ClsCurveStorage(**curve_args)
        ct.metadata = params  # 必须在设置curvefile前赋值
        params.update({'curveFile': ct.ObjectName})
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

        # 螺栓编码生成规则：控制器名称-job号-批次号
        result = params.get('result')
        controller_name = result.get('controller_name', None)
        job = result.get('job', None)
        batch_count = result.get('batch_count', None)
        pset = result.get('pset', None)
        bolt_number = generate_bolt_number(controller_name, job, batch_count, pset)

        try:
            craft_type = get_craft_type(bolt_number)
            _logger.info("craft_type: {}".format(craft_type))
        except Exception as e:
            _logger.error(e)
            craft_type = 1
            _logger.info('使用默认工艺类型：{}'.format(craft_type))

        if should_store:
            entity_id = params.get('entity_id')
            try:
                line_code, full_name = ResultStorageHook.get_line_code_by_controller_name(controller_name)
            except Exception as e:
                _logger.error(e)
                line_code = None

            from airflow.hooks.trigger_analyze_plugin import TriggerAnalyzeHook
            ResultStorageHook.save_result(
                entity_id,
                result,
                line_code=line_code,
                factory_code=params.get('factory_code', None),
                should_analyze=params.get('should_analyze'),
                bolt_number=bolt_number,
                device_type=result.get('device_type', 'tightening'),
                type=TriggerAnalyzeHook.get_result_type(params),
                craft_type=craft_type
            )

            ResultStorageHook.save_curve(params)
        _logger.info(params)
        params.update({
            'bolt_number': bolt_number,
            'craft_type': craft_type
        })
        return params

    # 根据entity_id更新分析结果
    @staticmethod
    def save_analyze_result(entity_id, analyze_result, **extra):
        st = ClsResultStorage()
        st.metadata = {
            'entity_id': entity_id
        }
        st.update(
            result=analyze_result,
            **extra
        )
        pass

    # 根据entity_id更新分析二次确认结果
    @staticmethod
    def save_final_state(entity_id, final_state, **extra):
        st = ClsResultStorage()
        st.metadata = {
            'entity_id': entity_id
        }
        st.update(
            final_state=final_state,
            **extra
        )

    # 在结果中保存task相关信息
    @staticmethod
    def bind_analyze_task(entity_id, dag_id, task_id, execution_date):
        st = ClsResultStorage()
        st.metadata = {
            'entity_id': entity_id
        }
        st.update(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date
        )


# Defining the plugin class
class ResultStoragePlugin(AirflowPlugin):
    name = "result_storage_plugin"
    hooks = [ResultStorageHook]
