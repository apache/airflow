from airflow.entities.result_mq import ClsResultMQ
import json
import pprint
from typing import Dict, Optional
from airflow.utils.logger import generate_logger
import os
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.utils import timezone
from airflow.models import DAG, Variable, DagRun
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
import datetime as dt
import pendulum
from distutils.util import strtobool
from airflow.utils.db import get_connection
from airflow.utils.curve import gen_template_key
import pika

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

DAG_ID = 'publish_result_dag'
TASK_ID = 'publish_result_task'


def onDagFail(context):
    _logger.error("{0} Run Fail".format(context))


def onDagSuccess(context):
    _logger.info("{0} Run Success".format(context))


local_tz = pendulum.timezone("Asia/Shanghai")

desoutter_default_args = {
    'owner': 'desoutter',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 1, 1, tzinfo=local_tz),
    'email': ['support@desoutter.cn'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': onDagFail,
    'on_success_callback': onDagSuccess,
    'on_retry_callback': None,
    'trigger_rule': 'all_success'
}


def format_analysis_result(entity_id: str, factory_code: str, result: str, verify_error: int, curve_mode: list) -> Dict:
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


def format_final_result(entity_id: str, factory_code: str, result: str, verify_error: int, curve_mode: list):
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


def format_tightening_result(tightening_result) -> Dict:
    entity_id = tightening_result.get('entity_id', None)
    factory_code = tightening_result.get('factory_code', '')
    result = tightening_result.get('result', {})
    curve = tightening_result.get('curve', {})
    craft_type = tightening_result.get('craft_type', None)
    curve_param = tightening_result.get('curve_param', {})
    nut_no = tightening_result.get('nut_no', None)
    template_cluster = tightening_result.get('template_cluster', {})
    task = tightening_result.get('task', {})
    version = tightening_result.get('version', None)
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


def format_tightening_result_for_hmi(tightening_result):
    task = tightening_result.get('task', {})
    execution_date = task.get('exec_date')
    dag_id = task.get('dag_id')
    task_id = task.get('task_id')
    date = timezone.parse(execution_date)
    ti = get_task_instance(dag_id, task_id, execution_date=date)
    return {
        'bolt_number': '{}/{}'.format(ti.bolt_number, ti.craft_type),
        'entity_id': ti.entity_id,
        'factory_code': ti.factory_code,
        'result': ti.measure_result,
        'car_code': ti.car_code,
    }


def format_analysis_result_for_hmi(analysis_result):
    result: str = analysis_result.get('result')
    dag_id = analysis_result.get('dag_id')
    task_id = analysis_result.get('task_id')
    execution_date = analysis_result.get('execution_date')
    date = timezone.parse(execution_date)
    ti = get_task_instance(dag_id, task_id, execution_date=date)
    return {
        'bolt_number': '{}/{}'.format(ti.bolt_number, ti.craft_type),
        'entity_id': ti.entity_id,
        'factory_code': ti.factory_code,
        'result': result,
        'car_code': ti.car_code,
        'verify_error': ti.verify_error,
        'curve_mode': ti.error_tag
    }


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


def do_push(data, queue):
    mq = ClsResultMQ(**get_result_mq_args())
    queue_config = Variable.get(queue, deserialize_json=True)
    if queue_config is None:
        raise Exception('config for queue "{}" missing'.format(queue))
    mq.send_message(
        json.dumps(data),
        **queue_config
    )


def format_analysis_result_to_mq(trigger_data) -> Optional[Dict]:
    try:
        result: str = trigger_data.get('result')
        dag_id = trigger_data.get('dag_id')
        task_id = trigger_data.get('task_id')
        execution_date = trigger_data.get('execution_date')
        date = timezone.parse(execution_date)
        ti = get_task_instance(dag_id, task_id, execution_date=date)
        data = format_analysis_result(
            entity_id=ti.entity_id,
            factory_code=ti.factory_code,
            result=result,
            verify_error=trigger_data.get('verify_error'),
            curve_mode=trigger_data.get('curve_mode')
        )
        return data
    except Exception as e:
        _logger.error("format_analysis_result_to_mq Error", e)
        return None


def send_analysis_result_to_mq(data):
    try:
        result: str = data.get('result', '')
        if PUSH_ANALYSIS_RESULT_MODE != 'ALL' and PUSH_ANALYSIS_RESULT_MODE != result:
            _logger.info('PUSH_ANALYSIS_RESULT_MODE is set to {}, skipping {} analysis results.'.format(
                PUSH_ANALYSIS_RESULT_MODE, result))
            return
        _logger.info('pushing analysis result to mq...')
        _logger.debug('pushing analysis result to mq Data: {}'.format(pprint.pformat(data, indent=4)))

        do_push(data, 'analysis_result_mq_queue')
        _logger.info('pushing analysis result to mq success')
    except Exception as e:
        _logger.error("push analysis result to mq failed: ".format(repr(e)))
        raise e


def send_final_result_to_mq(trigger_data):
    try:
        result = trigger_data.get('result')
        dag_id = trigger_data.get('dag_id')
        task_id = trigger_data.get('task_id')
        execution_date = trigger_data.get('execution_date')
        date = timezone.parse(execution_date)
        ti = get_task_instance(dag_id, task_id, execution_date=date)
        _logger.info('pushing final result to mq...')
        data = format_final_result(
            entity_id=ti.entity_id,
            factory_code=ti.factory_code,
            result=result,
            verify_error=trigger_data.get('verify_error'),
            curve_mode=trigger_data.get('curve_mode')
        )
        do_push(data, 'final_result_mq_queue')
        _logger.info('pushing final result to mq success')
    except Exception as e:
        _logger.error("push final result to mq failed: ".format(repr(e)))
        raise e


def send_curve_template_to_mq(data):
    try:
        template_name = data.get('template_name', None)
        template_data = data.get('template_data', None)
        if not template_name or not template_data:
            raise Exception('empty template name or template data')
        _logger.info('pushing curve_template: {}...'.format(template_name))
        do_push(format_template_data(template_name, template_data), 'curve_template_mq_queue')
        _logger.info('pushing curve template to mq success.')
    except Exception as e:
        _logger.error("push curve template to mq failed: ".format(repr(e)))
        raise e


def send_tightening_result_to_mq(tightening_result):
    try:
        _logger.info('pushing tightening result to mq...')
        data = tightening_result
        do_push(data, 'tightening_result_mq_queue')
        _logger.info('pushing tightening result to mq success')
    except Exception as e:
        _logger.error("push tightening result to mq failed: ".format(repr(e)))
        raise e


def verify_params(**kwargs):
    dag_run = kwargs.get('dag_run', None)
    params = None
    if isinstance(dag_run, DagRun):
        params = getattr(dag_run, 'conf')
    if isinstance(dag_run, dict):
        params = dag_run.get('conf', None)
    if params is None:
        raise Exception(u'参数params不存在')
    data_type = params.get('data_type')
    data = params.get('data')
    if not data_type or not data:
        raise Exception('empty data or data_type')
    return data_type, data


def send_templates_dict_to_mq(data):
    for key, value in data.items():
        send_curve_template_to_mq({
            'template_name': key,
            'template_data': value
        })

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



class SendResultHMIMixin(object):
    @classmethod
    def do_send_tightening_result_to_hmi_nd(cls, tightening_result: Dict, queue: str = 'tightening_result_mq_queue_nd'):
        md = cls.get_nd_mq_args()
        mq = ClsResultMQ(**md)
        if not queue:
            queue = 'tightening_result_mq_queue_nd'
        queue_config = Variable.get(queue, deserialize_json=True)
        if queue_config is None:
            raise Exception('config for queue "{}" missing'.format(queue))
        channel = get_channel(mq, **queue_config)
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
        channel = get_channel(mq, **queue_config)
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


def doPushResult(**kwargs):
    data_type, data = verify_params(**kwargs)
    if data_type == 'tightening_result':
        result = format_tightening_result(data)
        send_tightening_result_to_mq(result)
        if ENV_PUSH_HMI_ENABLE:
            send_tightening_result_to_hmi(format_tightening_result_for_hmi(data))
        return
    if data_type == 'analysis_result':
        format_data = format_analysis_result_to_mq(data)
        if not format_data:
            return
        send_analysis_result_to_mq(format_data)
        if ENV_PUSH_HMI_ENABLE:
            send_analysis_result_to_hmi(format_analysis_result_for_hmi(data))
        return
    if data_type == 'final_result':
        send_final_result_to_mq(data)
        return
    if data_type == 'curve_template':
        send_curve_template_to_mq(data)
        return
    if data_type == 'curve_templates_dict':
        send_templates_dict_to_mq(data)
        return


dag = DAG(
    dag_id=DAG_ID,
    description=u'上汽拧紧曲线分析结果推送',
    schedule_interval=schedule_interval,
    default_args=desoutter_default_args,
    max_active_runs=64,
    concurrency=64)

store_task = PythonOperator(provide_context=True,
                            task_id=TASK_ID, dag=dag,
                            python_callable=doPushResult)
