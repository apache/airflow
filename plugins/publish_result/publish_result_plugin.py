from abc import ABC
from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.plugins_manager import AirflowPlugin
from airflow.entities.result_mq import ClsResultMQ
import json
import pprint
from typing import Dict
from airflow.utils.logger import generate_logger
import os
from airflow.models import Variable
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from distutils.util import strtobool
from airflow.utils.db import get_connection
from plugins.utils import gen_template_key
import pika
from airflow.models import BaseOperator
from airflow.api.common.experimental import trigger_dag as trigger

_logger = LoggingMixin().log

RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')
try:
    ENV_PUSH_HMI_ENABLE = strtobool(os.getenv('ENV_PUSH_HMI_ENABLE', 'true'))
except:
    ENV_PUSH_HMI_ENABLE = False

if RUNTIME_ENV == 'prod':
    schedule_interval = None
    write_options = SYNCHRONOUS
else:
    schedule_interval = None
    write_options = ASYNCHRONOUS

_logger = generate_logger(__name__)

PUSH_ANALYSIS_RESULT_MODE = os.environ.get('PUSH_ANALYSIS_RESULT_MODE', 'ALL')  # 'OK', 'NOK', 'ALL'


class SendResultHMIMixin(object):
    @staticmethod
    def get_channel(mq, queue, **kwargs) -> pika.adapters.blocking_connection.BlockingChannel:
        if queue in mq.channels.keys():
            return mq.channels.get(queue)
        mq._connect()
        channel = mq._connection.channel()
        channel.confirm_delivery()
        passive = kwargs.get('passive', False)
        durable = kwargs.get('durable', False)
        exclusive = kwargs.get('exclusive', False)
        auto_delete = kwargs.get('auto_delete', False)
        arguments = kwargs.get('arguments', None)
        channel.queue_declare(
            queue,
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments
        )
        exchange = kwargs.get('exchange', None)
        exchange_type = kwargs.get('exchange_type', 'fanout')
        exchange_durable = kwargs.get('exchange_durable', None)

        if exchange and exchange_type:
            channel.exchange_declare(exchange=exchange, exchange_type=exchange_type, durable=exchange_durable)
            channel.queue_bind(exchange=exchange,
                               queue=queue,
                               routing_key=kwargs.get('routing_key', '#'))  # 匹配python.后所有单词
        mq.channels[queue] = channel  # 将channel加入到字典对象中
        return channel

    @classmethod
    def do_send_tightening_result_to_hmi_nd(cls, tightening_result: Dict, queue: str = 'tightening_result_mq_queue_nd'):
        md = cls.get_nd_mq_args()
        mq = ClsResultMQ(**md)
        if not queue:
            queue = 'tightening_result_mq_queue_nd'
        queue_config = Variable.get(queue, deserialize_json=True)
        if queue_config is None:
            raise Exception('config for queue "{}" missing'.format(queue))
        channel = SendResultHMIMixin.get_channel(mq, **queue_config)
        exchange = queue_config.get('exchange', None)
        channel.basic_publish(exchange=exchange, routing_key=queue_config.get('routing_key', ''),
                              body=json.dumps([tightening_result]),
                              properties=pika.BasicProperties(
                                  headers={'msgType': queue_config.get('msgType', '')},
                                  content_type="application/json"
                              ))

    @classmethod
    def do_send_analysis_result_to_hmi_nd(cls, data: Dict, queue: str = 'analysis_result_mq_queue_nd'):
        md = cls.get_nd_mq_args()
        mq = ClsResultMQ(**md)
        if not queue:
            queue = 'analysis_result_mq_queue_nd'
        queue_config = Variable.get(queue, deserialize_json=True)
        if queue_config is None:
            raise Exception('config for queue "{}" missing'.format(queue))
        channel = SendResultHMIMixin.get_channel(mq, **queue_config)
        exchange = queue_config.get('exchange', None)
        channel.basic_publish(exchange=exchange, routing_key=queue_config.get('routing_key', ''),
                              body=json.dumps([data]),
                              properties=pika.BasicProperties(
                                  headers={'msgType': queue_config.get('msgType', '')},
                                  content_type="application/json"
                              ))

    @classmethod
    def get_nd_mq_args(cls):
        mq = get_connection('qcos_rabbitmq_nd')
        data = {
            "host": mq.host if mq else None,
            "port": mq.port if mq else None,
            "username": mq.login if mq else None,
            "password": mq.get_password() if mq else None
        }
        data.update(mq.extra_dejson)
        return data


class PublishResultHook(BaseHook, ABC):
    @staticmethod
    def format_analysis_result(entity_id: str = '', factory_code: str = '', result: str = '', verify_error: int = 0,
                               curve_mode: list = None, **kwargs) -> Dict:
        if curve_mode is None:
            curve_mode = []
        return {
            'entity_id': entity_id,
            'factory_code': factory_code,
            'type': 'analysis_result',
            'result': result,
            'verify_error': verify_error,
            'curve_mode': json.dumps(curve_mode)
        }

    @staticmethod
    def format_final_result(entity_id: str = '', factory_code: str = '', result: str = '', verify_error: int = 0,
                            curve_mode: list = None, **kwargs):
        if curve_mode is None:
            curve_mode = []
        return {
            'entity_id': entity_id,
            'factory_code': factory_code,
            'type': 'final_result',
            'result': result,
            'verify_error': verify_error,
            'curve_mode': json.dumps(curve_mode)
        }

    @staticmethod
    def format_tightening_result(entity_id=None, factory_code='', result=None, curve=None, craft_type=None,
                                 curve_param=None, nut_no=None, template_cluster=None, task=None,
                                 version=None, **kwargs) -> Dict:
        return {
            'entity_id': entity_id,
            'factory_code': factory_code,
            'result': result,
            'curve': json.dumps(curve),
            'craft_type': craft_type,
            'curve_param': json.dumps(curve_param),
            'nut_no': nut_no,
            'template_cluster': json.dumps(template_cluster),
            'task': json.dumps(task),
            'version': version,
        }

    @staticmethod
    def format_tightening_result_for_hmi(
        bolt_number='',
        craft_type='',
        result=None,
        entity_id='',
        factory_code='',
    ):
        return {
            'bolt_number': '{}/{}'.format(bolt_number, craft_type),
            'entity_id': entity_id,
            'factory_code': factory_code,
            'result': result.get('measure_result', '') if result else '',
            'car_code': result.get('vin', '') if result else '',
        }

    @staticmethod
    def format_analysis_result_for_hmi(
        bolt_number='',
        craft_type='',
        entity_id='',
        factory_code='',
        car_code='',
        verify_error='',
        error_tag='',
        **kwargs
    ):
        result: str = kwargs.get('result')
        return {
            'bolt_number': '{}/{}'.format(bolt_number, craft_type),
            'entity_id': entity_id,
            'factory_code': factory_code,
            'result': result,
            'car_code': car_code,
            'verify_error': verify_error,
            'curve_mode': error_tag
        }

    @staticmethod
    def format_template_data(template_name, template_data):
        data = template_data
        if isinstance(template_data, str):
            try:
                data = json.loads(template_data)
            except Exception:
                _logger.log('cannot decode template data as json string, sending original data...')
        data.update({
            'curve_param': json.dumps(data.get('curve_param', {})),
            'template_cluster': json.dumps(data.get('template_cluster', {}))
        })
        return {
            'template_name': gen_template_key(template_name),
            'template_data': data
        }

    @staticmethod
    def get_result_mq_args():
        mq = get_connection('qcos_rabbitmq')
        data = {
            "host": mq.host if mq else None,
            "port": mq.port if mq else None,
            "username": mq.login if mq else None,
            "password": mq.get_password() if mq else None
        }
        data.update(mq.extra_dejson)
        return data

    @staticmethod
    def do_push(data, queue):
        mq = ClsResultMQ(**PublishResultHook.get_result_mq_args())
        queue_config = Variable.get(queue, deserialize_json=True)
        if queue_config is None:
            raise Exception('config for queue "{}" missing'.format(queue))
        mq.send_message(
            json.dumps(data),
            **queue_config
        )

    @staticmethod
    def send_analysis_result_to_mq(data):
        try:
            result: str = data.get('result', '')
            if PUSH_ANALYSIS_RESULT_MODE != 'ALL' and PUSH_ANALYSIS_RESULT_MODE != result:
                _logger.info('PUSH_ANALYSIS_RESULT_MODE is set to {}, skipping {} analysis results.'.format(
                    PUSH_ANALYSIS_RESULT_MODE, result))
                return
            _logger.info('pushing analysis result to mq...')
            _logger.debug('pushing analysis result to mq Data: {}'.format(pprint.pformat(data, indent=4)))

            PublishResultHook.do_push(data, 'analysis_result_mq_queue')
            _logger.info('pushing analysis result to mq success')
        except Exception as e:
            _logger.error("push analysis result to mq failed: ".format(repr(e)))
            raise e

    @staticmethod
    def send_final_result_to_mq(
        result=None,
        entity_id=None,
        factory_code=None,
        verify_error=None,
        curve_mode=None,
        **kwargs
    ):
        try:
            _logger.info('pushing final result to mq...')
            data = PublishResultHook.format_final_result(
                entity_id=entity_id,
                factory_code=factory_code,
                result=result,
                verify_error=verify_error,
                curve_mode=curve_mode
            )
            PublishResultHook.do_push(data, 'final_result_mq_queue')
            _logger.info('pushing final result to mq success')
        except Exception as e:
            _logger.error("push final result to mq failed: ".format(repr(e)))
            raise e

    @staticmethod
    def send_tightening_result_to_mq(tightening_result):
        try:
            _logger.info('pushing tightening result to mq...')
            data = tightening_result
            PublishResultHook.do_push(data, 'tightening_result_mq_queue')
            _logger.info('pushing tightening result to mq success')
        except Exception as e:
            _logger.error("push tightening result to mq failed: ".format(repr(e)))
            raise e

    @staticmethod
    def send_tightening_result_to_hmi(tightening_result: Dict):
        _logger.info("Sending tightening result to hmi, data: {}".format(tightening_result))
        factory_code: str = tightening_result.get('factory_code', '')
        if not factory_code:
            _logger.error("Can Not Found Factory Code To Push To HMI")
            return
        method = 'do_send_tightening_result_to_hmi_{}'.format(factory_code.lower())
        try:
            has_method = hasattr(SendResultHMIMixin, method)
            if has_method:
                m = getattr(SendResultHMIMixin, method)
                m(tightening_result)
        except Exception as e:
            _logger.error("Get Push Result To Hmi Method Error", e)

    @staticmethod
    def send_analysis_result_to_hmi(data: Dict):
        _logger.info("Sending analysis result to hmi, data: {}".format(data))
        factory_code: str = data.get('factory_code', '')
        if not factory_code:
            _logger.error("Can Not Found Factory Code To Push To HMI")
            return
        method = 'do_send_analysis_result_to_hmi_{}'.format(factory_code.lower())
        try:
            has_method = hasattr(SendResultHMIMixin, method)
            if has_method:
                m = getattr(SendResultHMIMixin, method)
                m(data)
        except Exception as e:
            _logger.error("Get Push Result To Hmi Method Error", e)

    @staticmethod
    def publish_tightening_result(**data):
        result = PublishResultHook.format_tightening_result(**data)
        PublishResultHook.send_tightening_result_to_mq(result)
        if ENV_PUSH_HMI_ENABLE:
            PublishResultHook.send_tightening_result_to_hmi(PublishResultHook.format_tightening_result_for_hmi(data))

    @staticmethod
    def publish_analysis_result(**data):
        format_data = PublishResultHook.format_analysis_result(**data)
        if not format_data:
            return
        PublishResultHook.send_analysis_result_to_mq(format_data)
        if ENV_PUSH_HMI_ENABLE:
            PublishResultHook.send_analysis_result_to_hmi(PublishResultHook.format_analysis_result_for_hmi(**data))

    @staticmethod
    def send_curve_template_to_mq(template_name=None, template_data=None, **kwargs):
        try:
            if not template_name or not template_data:
                raise Exception('empty template name or template data')
            _logger.info('pushing curve_template: {}...'.format(template_name))
            PublishResultHook.do_push(PublishResultHook.format_template_data(template_name, template_data),
                                      'curve_template_mq_queue')
            _logger.info('pushing curve template to mq success.')
        except Exception as e:
            _logger.error("push curve template to mq failed: ".format(repr(e)))
            raise e

    @staticmethod
    def send_templates_dict_to_mq(**data):
        for key, value in data.items():
            PublishResultHook.send_curve_template_to_mq(
                template_name=key,
                template_data=value
            )

    @staticmethod
    def do_publish(data_type, data):
        data_type_handlers = {
            'tightening_result': PublishResultHook.publish_tightening_result,
            'analysis_result': PublishResultHook.publish_analysis_result,
            'final_result': PublishResultHook.send_final_result_to_mq,
            'curve_template': PublishResultHook.send_curve_template_to_mq,
            'curve_templates_dict': PublishResultHook.send_templates_dict_to_mq
        }
        handler = data_type_handlers.get(data_type)
        if handler is None:
            _logger.error('推送结果不支持结果类型：{}'.format(data_type))
            return
        handler(**data)

    @staticmethod
    def trigger_publish(data_type, data):
        push_result_dat_id = 'publish_result_dag'
        conf = {
            'data': data,
            'data_type': data_type
        }
        trigger.trigger_dag(
            push_result_dat_id,
            conf=conf,
            replace_microseconds=False
        )


class PublishResultOperator(BaseOperator):
    @staticmethod
    def verify_params(params):
        if params is None:
            raise Exception(u'参数params不存在')
        data_type = params.get('data_type')
        data = params.get('data')
        if not data_type or not data:
            raise Exception('empty data or data_type')
        return data_type, data

    def execute(self, context):
        params = context['dag_run'].conf
        from airflow.hooks.publish_result_plugin import PublishResultHook
        data_type, data = PublishResultOperator.verify_params(params)
        PublishResultHook.do_publish(data_type, data)


# Defining the plugin class
class PublishResultPlugin(AirflowPlugin):
    name = "publish_result_plugin"
    operators = [PublishResultOperator]
    hooks = [PublishResultHook]
