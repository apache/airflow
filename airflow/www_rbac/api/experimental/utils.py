from airflow import models
from airflow.utils.db import create_session
from airflow.utils.logger import generate_logger
import os
from airflow.models.variable import Variable
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

CAS_BASE_URL = os.environ.get("CAS_BASE_URL", "http://localhost:9095")
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

DEFAULT_CRAFT_TYPE = os.environ.get('CRAFT_TYPE', '1')

if RUNTIME_ENV == 'prod':
    schedule_interval = None
    write_options = SYNCHRONOUS
else:
    schedule_interval = None
    write_options = ASYNCHRONOUS

_logger = generate_logger(__name__)

def get_cas_base_url():
    connection_model = models.connection.Connection
    with create_session() as session:
        cas_base_url = session.query(connection_model).filter(
            connection_model.conn_id == 'cas_base_url').first()
    if not cas_base_url:
        cas_base_url = CAS_BASE_URL  # 从环境变量中获取URL配置
    return cas_base_url.get_uri() if isinstance(cas_base_url, connection_model) else cas_base_url


def get_craft_type(craft_type: str = DEFAULT_CRAFT_TYPE) -> int:
    ret = CRAFT_TYPE_MAP.get(craft_type, None)
    if ret:
        return ret
    else:
        return CRAFT_TYPE_MAP.get(DEFAULT_CRAFT_TYPE, None)


def get_curve_mode(measure_result: str) -> int:
    scurveMode = measure_result.upper()
    if scurveMode not in CURVE_MODE_MAP.keys():
        return CURVE_MODE_MAP.get('OK')
    else:
        return CURVE_MODE_MAP.get(scurveMode)


def generate_bolt_number(controller_name, program, batch_count=None):
    if not controller_name or not program or batch_count is None or batch_count is '':
        raise BaseException(u'{}参数未正确定义'.format('generateBoltNumber'))
    if not isinstance(program, str):
        program = str(program)
    return '-'.join([controller_name, program, str(batch_count)])


def generate_curve_name(nut_no, measure_result):
    return '/'.join([nut_no, measure_result, str(get_craft_type())])


def get_curve_params(bolt_number, measure_result):
    curve_name = generate_curve_name(bolt_number, measure_result)
    try:
        return Variable.get_fuzzy_active(
            curve_name,
            deserialize_json=True,
        )[1]
    except Exception as e:
        _logger.error("cannot get curve params :{0} ".format(str(e)))
        return {}


def get_task_params(task_instance, entity_id):
    task = {
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "real_task_id": entity_id,
        "exec_date": '{}'.format(task_instance.execution_date)
    }
    return {'task': task}


def get_result_args():
    return {
        "bucket": 'desoutter',
        "url": Variable.get('influxdb_url', '10.1.1.44:9999'),
        "ou": 'desoutter',
        "token": Variable.get('influxdb_token',
                              'PP4zVtAxld9oOISOTeWx0uuVXUQfvHi8hnFe47U-pef70eh8eaKzfxlVv0dUuggoXe4-3WOnedV3u-xp2-5sQ=='),
        # "token": Variable.get('influxdb_token', 'token'),
        'write_options': write_options
    }


def get_curve_args():
    return {
        "bucket": "desoutter",
        "endpoint": Variable.get('oss_url', '10.1.1.44:9000'),
        "access_key": Variable.get('oss_key', 'minio'),
        "secret_key": Variable.get('oss_secret', 'minio123'),
        "secure": False
    }
