import asyncio
from airflow.utils import apply_defaults
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.curve import get_curve_params, get_task_params, generate_bolt_number, \
    get_craft_type
from airflow.models.tightening_controller import TighteningController
from typing import Callable, Dict, List, Optional
from airflow.api.common.experimental.mark_tasks import modify_task_instance
from airflow.api.common.experimental import trigger_dag as trigger

_logger = LoggingMixin().log


def trigger_push_result_dag(params):
    _logger.info('pushing result to mq...')
    push_result_dat_id = 'publish_result_dag'
    conf = {
        'data': params,
        'data_type': 'tightening_result'
    }
    trigger.trigger_dag(push_result_dat_id, conf=conf,
                        replace_microseconds=False)


def get_line_code_by_controller_name(controller_name):
    controller_data = TighteningController.find_controller(controller_name)
    if not controller_data:
        raise Exception('未找到控制器数据: {}'.format(controller_name))
    controller = '{}@{}/{}'.format(controller_data.get('controller_name'),
                                   controller_data.get('work_center_code'),
                                   controller_data.get('work_center_name'))
    return controller_data.get('line_code', None), controller


def is_rework_result(params: Dict) -> bool:
    result_body = params.get('result', None)
    if not result_body:
        return False
    job = result_body.get('job', 0)
    batch_count = result_body.get('batch_count', 0)
    if (not job) and (not batch_count):
        return True
    return False


def get_task_instance_type(params: Dict) -> str:
    ret = 'normal'
    if is_rework_result(params):
        return 'rework'
    return ret


class TriggerAnalyzeOperator(BaseOperator):

    def prepare_trigger_params(self, params, task_instance):
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

    def execute(self, context):
        params = context['dag_run'].conf
        task_instance = context['task_instance']
        new_param = self.prepare_trigger_params(params, task_instance)
        trigger_push_result_dag(new_param)
        from airflow.hooks.cas_plugin import CasHook
        cas = CasHook(role='analysis')
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(cas.trigger_analyze(new_param))
        loop.close()
        return result


# Defining the plugin class
class TriggerAnalyzePlugin(AirflowPlugin):
    name = "trigger_analyze_plugin"
    operators = [TriggerAnalyzeOperator]
