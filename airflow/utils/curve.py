from airflow import models
from airflow.utils.db import create_session, get_connection
from airflow.utils.logger import generate_logger
import os
from airflow.models.variable import Variable
from airflow.models.taskinstance import TaskInstance
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from airflow.entities.result_storage import ClsResultStorage
from airflow.entities.curve_storage import ClsCurveStorage
from airflow.api.common.experimental import trigger_dag as trigger
import json
from typing import Dict, Optional, Union
from airflow.exceptions import AirflowException, AirflowNotFoundException, AirflowConfigException
from airflow.utils import timezone
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.utils.db import provide_session

CAS_ANALYSIS_BASE_URL = os.environ.get("CAS_ANALYSIS_BASE_URL", "http://localhost:9095")
CAS_TRAINING_BASE_URL = os.environ.get("CAS_TRAINING_BASE_URL", "http://localhost:9095")
RUNTIME_ENV = os.environ.get('RUNTIME_ENV', 'dev')

CRAFT_TYPE_MAP = {
    '1': 1,
    '2': 2,
    '4': 4
}

CURVE_MODE_MAP = {
    'OK': 0,
    'NOK': 1,
}

if RUNTIME_ENV == 'prod':
    schedule_interval = None
    write_options = SYNCHRONOUS
else:
    schedule_interval = None
    write_options = ASYNCHRONOUS

_logger = generate_logger(__name__)


def ensure_int(num):
    try:
        return int(num)
    except Exception as e:
        _logger.error(e)
        return num


def get_cas_analysis_base_url():
    connection_model = models.connection.Connection
    with create_session() as session:
        url = session.query(connection_model).filter(
            connection_model.conn_id == 'cas_analysis').first()
    if not url:
        url = CAS_ANALYSIS_BASE_URL  # 从环境变量中获取URL配置
    return url.get_uri() if isinstance(url, connection_model) else url


def get_cas_training_base_url():
    connection_model = models.connection.Connection
    with create_session() as session:
        url = session.query(connection_model).filter(
            connection_model.conn_id == 'cas_training').first()
    if not url:
        url = CAS_TRAINING_BASE_URL  # 从环境变量中获取URL配置
    return url.get_uri() if isinstance(url, connection_model) else url


def get_craft_type(nut_no: str) -> Optional[int]:
    template_data = Variable.get_fuzzy_active(nut_no,
                                              deserialize_json=True,
                                              default_var=None
                                              )[1]
    ret = template_data.get('craft_type', None)
    if ret:
        return ret
    else:
        raise AirflowNotFoundException(u'没有找到螺栓对应的工艺类型')


def get_curve_mode(final_state, error_tag):
    train_error_tag = os.environ.get('TRAIN_ERROR_TAG', False)
    if final_state == 'OK':
        return [0]
    if train_error_tag == 'False' or train_error_tag is False:
        return [1]
    if error_tag is not None:
        curve_modes = json.loads(error_tag)
        if len(curve_modes) > 0:
            return list(map(ensure_int, curve_modes))
    return None


def generate_bolt_number(controller_name, program, batch_count, pset):
    if not controller_name or program is None \
        or batch_count is None or batch_count == '' \
        or pset is None or pset == '':
        raise AirflowConfigException(u'{}参数未正确定义'.format('generateBoltNumber'))
    if not isinstance(program, str):
        program = str(program)
    return '_'.join([controller_name, program, str(batch_count), str(pset)])


def generate_curve_name(nut_no):
    return '/'.join([nut_no, str(get_craft_type(nut_no))])


def get_curve_params(bolt_number):
    curve_name = generate_curve_name(bolt_number)
    try:
        return Variable.get_fuzzy_active(
            curve_name,
            deserialize_json=True,
            default_var={}
        )[1]
    except Exception as e:
        _logger.error("cannot get curve params :{0} ".format(repr(e)))
        return {}


def get_task_params(task_instance, entity_id):
    task = {
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "real_task_id": entity_id,
        "exec_date": '{}'.format(task_instance.execution_date)
    }
    return {'task': task}


def get_result_args(connection_key='qcos_influxdb'):
    influxdb = get_connection(connection_key)
    extra = influxdb.extra_dejson if influxdb else {}
    return {
        "bucket": extra.get('bucket', 'desoutter'),
        "url": '{}:{}'.format(influxdb.host, influxdb.port) if influxdb else None,
        "ou": extra.get('ou', 'desoutter'),
        "token": influxdb.get_password() if influxdb else None,
        'write_options': write_options
    }


def get_kafka_consumer_args(connection_key: str ='qcos_kafka_consumer'):
    kafka_conn = get_connection(connection_key)
    extra = kafka_conn.extra_dejson if kafka_conn else {}
    return {
        "bootstrap_servers": extra.get('bootstrap_servers', 'localhost:9092'),
        'security_protocol': extra.get('security_protocol'),
        'auth_type':  extra.get('auth_type'),
        "user": kafka_conn.login or '',
        "password": kafka_conn.get_password() if kafka_conn else ''
    }


def get_curve_args(connection_key='qcos_minio'):
    oss = get_connection(connection_key)
    extra = oss.extra_dejson if oss else {}
    return {
        "bucket": extra.get('bucket', 'desoutter'),
        "endpoint": '{}:{}'.format(oss.host, oss.port) if oss else None,
        "access_key": oss.login if oss else None,
        "secret_key": oss.get_password() if oss else None,
        "secure": extra.get('secure', False),
    }


def form_analysis_result_trigger(result, entity_id, execution_date, task_id, dag_id, verify_error, curve_mode):
    return {
        'result': result,
        'entity_id': entity_id,
        'execution_date': execution_date,
        'task_id': task_id,
        'verify_error': verify_error,
        'curve_mode': curve_mode,
        'dag_id': dag_id,
    }


def get_curve_entity_ids(bolt_number=None, craft_type=None):
    tasks = TaskInstance.list_tasks(craft_type, bolt_number)
    tasks.sort(key=lambda t: t.execution_date, reverse=True)
    return list(map(lambda ti: ti.entity_id, tasks))


def get_analysis_tasks(bolt_number=None, craft_type=None):
    tasks = TaskInstance.query_tasks(craft_type, bolt_number).order_by(TaskInstance.execution_date.desc())
    return tasks


def trigger_push_result_to_mq(data_type, result, entity_id, execution_date, task_id, dag_id, verify_error, curve_mode):
    if isinstance(curve_mode, str):
        curve_mode = json.loads(curve_mode)
    if isinstance(curve_mode, int):
        curve_mode = [curve_mode]
    if curve_mode is None:
        curve_mode = []
    analysis_result = form_analysis_result_trigger(
        result,
        entity_id,
        execution_date,
        task_id,
        dag_id,
        verify_error,
        curve_mode
    )
    push_result_dag_id = 'publish_result_dag'
    conf = {
        'data': analysis_result,
        'data_type': data_type
    }
    trigger.trigger_dag(push_result_dag_id, conf=conf, replace_microseconds=False)


def trigger_training_dag(dag_id, task_id, execution_date, final_state, error_tags):
    trigger_training_dag_id = 'curve_training_dag'
    conf = {
        'dag_id': dag_id,
        'task_id': task_id,
        'execution_date': execution_date,
        'final_state': final_state,
        'error_tags': error_tags
    }
    trigger.trigger_dag(trigger_training_dag_id, conf=conf, replace_microseconds=False)


def get_result(entity_id):
    st = ClsResultStorage(**get_result_args())
    st.metadata = {'entity_id': entity_id}
    result = st.query_result()
    return result if result else {}


def get_results(entity_ids):
    st = ClsResultStorage(**get_result_args())
    if not isinstance(entity_ids, list):
        return []
    st.metadata = {'entity_id': entity_ids}
    result = st.query_results()  # 查询多条记录
    return result if result else []


def get_curve(entity_id):
    st = ClsCurveStorage(**get_curve_args())
    st.metadata = {'entity_id': entity_id}
    return st.query_curve()


@provide_session
def get_task_instances_by_entity_ids(entity_ids, session=None):
    tis = session.query(TaskInstance).filter(
        TaskInstance.entity_id.in_(entity_ids),
        TaskInstance.task_id == 'trigger_anay_task'
    ).all()
    return tis


def trigger_push_template_dag(template_name, template_data):
    push_result_dag_id = 'publish_result_dag'
    conf = {
        'data': {
            'template_name': template_name,
            'template_data': template_data
        },
        'data_type': 'curve_template'
    }
    trigger.trigger_dag(push_result_dag_id, conf=conf, replace_microseconds=False)


def trigger_push_templates_dict_dag(templates_dict):
    push_result_dag_id = 'publish_result_dag'
    conf = {
        'data': templates_dict,
        'data_type': 'curve_templates_dict'
    }
    trigger.trigger_dag(push_result_dag_id, conf=conf, replace_microseconds=False)


def do_save_curve_error_tag(dag_id, task_id, execution_date, error_tags=None):
    try:
        execution_date = timezone.parse(execution_date)
    except ValueError:
        error_message = (
            'Given execution date, {}, could not be identified '
            'as a date. Example date format: 2015-11-16T14:34:15+00:00'
                .format(execution_date))
        raise AirflowException(error_message)
    if error_tags is None:
        error_tags = []
    task = get_task_instance(dag_id, task_id, execution_date)
    task.set_error_tag(json.dumps(error_tags))


def should_trigger_training(result, final_state, analysis_mode, train_mode):
    ENV_TRIGGER_TRAINING_MODE = os.environ.get('TRIGGER_TRAINING_MODE', 'ANALYSIS_ERROR')
    # ANALYSIS_ERROR, ALWAYS, DIFFERENT_MODE
    if ENV_TRIGGER_TRAINING_MODE == 'ALWAYS':
        return True
    if ENV_TRIGGER_TRAINING_MODE == 'DIFFERENT_MODE':
        modes = json.loads(analysis_mode)
        train_modes = json.loads(train_mode)
        if len(modes) != len(train_modes):
            return True
        for mode in modes:
            if mode not in train_mode:
                return True
        return False
    return final_state != result


def get_curve_template_name(key: str) -> str:
    if '@@' in key:
        return key.split('@@')[0]
    return key


# to redis event key
def gen_template_key(template_name):
    template_name = get_curve_template_name(template_name)
    template_prefix = os.environ.get('TEMPLATE_KEY_PREFIX', 'qcos_templates')
    if '{}.'.format(template_prefix) in template_name:
        return template_name
    return "{}.{}".format(template_prefix, template_name)


# from redis event key
def parse_template_name(template_key):
    return template_key.split(".")[1]
