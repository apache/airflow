import os
import asyncio
from abc import ABC

from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from plugins.utils import get_curve_params, get_task_params, generate_bolt_number, \
    get_craft_type
from airflow.models.tightening_controller import TighteningController
from typing import Dict
from airflow.api.common.experimental import trigger_dag as trigger
from distutils.util import strtobool

_logger = LoggingMixin().log

try:
    ENV_ALWAYS_TRIGGER_ANAY = strtobool(
        os.environ.get('ENV_ALWAYS_TRIGGER_ANAY', 'true'))
except:
    ENV_ALWAYS_TRIGGER_ANAY = True


class TriggerAnalyzeHook(BaseHook, ABC):

    @staticmethod
    def trigger_push_result_dag(params):
        _logger.info('pushing result to mq...')
        push_result_dat_id = 'publish_result_dag'
        conf = {
            'data': params,
            'data_type': 'tightening_result'
        }
        trigger.trigger_dag(push_result_dat_id, conf=conf,
                            replace_microseconds=False)

    @staticmethod
    def is_rework_result(params: Dict) -> bool:
        result_body = params.get('result', None)
        if not result_body:
            return False
        job = result_body.get('job', 0)
        batch_count = result_body.get('batch_count', 0)
        if (not job) and (not batch_count):
            return True
        return False

    @staticmethod
    def get_result_type(params: Dict) -> str:
        ret = 'normal'
        if TriggerAnalyzeHook.is_rework_result(params):
            return 'rework'
        return ret

    @staticmethod
    def should_skip_analysis(params: Dict) -> bool:
        should_analyze = params.get('should_analyze', True)
        if not should_analyze:
            _logger.info('接收到不分析指令，跳过分析...')
            return True
        if ENV_ALWAYS_TRIGGER_ANAY:
            return False
        if TriggerAnalyzeHook.is_rework_result(params):
            _logger.info('返修工位曲线，跳过分析...')
            return True
        return False

    @staticmethod
    def prepare_trigger_params(params, task_instance):
        result_body = params.get('result', None)
        entity_id = params.get('entity_id', None)
        new_param = params.copy()
        # 螺栓编码生成规则：控制器名称-job号-批次号
        controller_name = result_body.get('controller_name', None)
        job = result_body.get('job', None)
        batch_count = result_body.get('batch_count', None)
        pset = result_body.get('pset', None)
        bolt_number = generate_bolt_number(controller_name, job, batch_count, pset)
        from airflow.hooks.result_storage_plugin import ResultStorageHook
        line_code, full_name = ResultStorageHook.get_line_code_by_controller_name(controller_name)
        result_type = TriggerAnalyzeHook.get_result_type(new_param)
        _logger.info("type: {}, entity_id: {}, line_code: {}, bolt_number: {}"
                     .format(result_type, entity_id, line_code, bolt_number))

        try:
            craft_type = get_craft_type(bolt_number)
            _logger.info("craft_type: {}".format(craft_type))
        except Exception as e:
            _logger.error(e)
            craft_type = 1
            _logger.info('使用默认工艺类型：{}'.format(craft_type))

        try:
            curve_params = get_curve_params(bolt_number)
        except Exception as e:
            _logger.error(e)
            curve_params = {}
            _logger.error('无法获取曲线参数（{}/{}）'.format(bolt_number, craft_type))

        task_params = get_task_params(task_instance, entity_id)
        new_param.update(curve_params)
        new_param.update(task_params)
        new_param.update({'craft_type': craft_type})
        return new_param

    @staticmethod
    def do_trigger_analyze(params, task_instance):
        should_skip_analysis = TriggerAnalyzeHook.should_skip_analysis(params)
        if should_skip_analysis:
            return
        new_param = TriggerAnalyzeHook.prepare_trigger_params(params, task_instance)
        TriggerAnalyzeHook.trigger_push_result_dag(new_param)
        from airflow.hooks.cas_plugin import CasHook
        cas = CasHook(role='analysis')
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(cas.trigger_analyze(new_param))
        loop.close()
        return result

    @staticmethod
    def trigger_analyze(params):
        # 此处未来或将不创建分析任务。
        # 添加此方法意在统一外部接口，不在不同地方调用trigger_dag，便于维护
        trigger.trigger_dag('curve_analyze_dag', conf=params, replace_microseconds=False)


class TriggerAnalyzeOperator(BaseOperator):

    def execute(self, context):
        params = context['dag_run'].conf
        task_instance = context['task_instance']
        from airflow.hooks.trigger_analyze_plugin import TriggerAnalyzeHook
        TriggerAnalyzeHook.do_trigger_analyze(params, task_instance)


# Defining the plugin class
class TriggerAnalyzePlugin(AirflowPlugin):
    name = "trigger_analyze_plugin"
    operators = [TriggerAnalyzeOperator]
    hooks = [TriggerAnalyzeHook]
