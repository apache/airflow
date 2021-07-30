import os
import asyncio
from abc import ABC
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from plugins.utils import get_curve_params
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
        from airflow.hooks.publish_result_plugin import PublishResultHook
        PublishResultHook.trigger_publish('tightening_result', params)

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
    def prepare_trigger_params(params):
        result_body = params.get('result', None)
        entity_id = params.get('entity_id', None)
        bolt_number = params.get('bolt_number', None)
        craft_type = params.get('craft_type', None)
        assert result_body is not None, '触发分析的数据中拧紧结果为空'
        assert entity_id is not None, '触发分析的数据中entity_id为空'
        assert bolt_number is not None, '触发分析的数据中bolt_number为空'
        assert craft_type is not None, '触发分析的数据中craft_type为空'
        new_param = params.copy()
        # 螺栓编码生成规则：控制器名称-job号-批次号
        result_type = TriggerAnalyzeHook.get_result_type(new_param)
        _logger.info("type: {}, entity_id: {} bolt_number: {}"
                     .format(result_type, entity_id, bolt_number))

        try:
            curve_params = get_curve_params(bolt_number)
        except Exception as e:
            _logger.error(e)
            curve_params = {}
            _logger.error('无法获取曲线参数（{}/{}）'.format(bolt_number, craft_type))

        new_param.update(curve_params)
        return new_param

    @staticmethod
    def do_trigger_analyze(params, task_instance):
        should_skip_analysis = TriggerAnalyzeHook.should_skip_analysis(params)
        if should_skip_analysis:
            return
        from airflow.hooks.result_storage_plugin import ResultStorageHook
        entity_id = params.get('entity_id', None)
        ResultStorageHook.bind_analyze_task(
            entity_id,
            task_instance.dag_id,
            task_instance.task_id,
            task_instance.execution_date
        )
        new_param = TriggerAnalyzeHook.prepare_trigger_params(params)
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
