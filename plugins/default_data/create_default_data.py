from airflow.utils.db import provide_session
import os
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.plugins_manager import AirflowPlugin
from plugins.utils.load_data_from_csv import load_data_from_csv
from airflow.configuration import conf
from plugins.factory_code.factory_code import get_factory_code
import json

log = LoggingMixin().log


@provide_session
def load_default_controller(file_dir, session=None):
    log.info("Loading default controllers")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(
        current_dir,
        f'data/{file_dir}/default_controllers.csv' if file_dir else 'data/default_controllers.csv'
    )
    if not os.path.exists(file_path):
        log.error("导入控制器目录不存在：{}".format(file_path))
        return
    from plugins.models.tightening_controller import TighteningController

    val = load_data_from_csv(file_path)
    for controller in val:
        if TighteningController.controller_exists(**controller):
            log.info(f"Controller already exists, skipping, {repr(controller)}")
            continue
        TighteningController.add_controller(session=session, **controller)


@provide_session
def merge_data(model, data, is_exist, session=None):
    if not session.query(model).filter(is_exist).first():
        session.add(data)
        session.commit()


@provide_session
def create_default_error_tags(session=None):
    log.info("Loading default error_tags")
    from plugins.models.error_tag import ErrorTag
    current_dir = os.path.dirname(os.path.abspath(__file__))
    error_tags = load_data_from_csv(os.path.join(current_dir, 'data/error_tags.csv'), {
        'value': 'value',
        'label': 'label'
    })
    for error_tag in error_tags:
        data = ErrorTag(label=error_tag.get('label'), value=error_tag.get('value'))
        merge_data(model=ErrorTag, data=data, is_exist=ErrorTag.label == data.label, session=session)


@provide_session
def create_device_type_support(session=None):
    log.info("Loading default device types")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if not session:
        return
    device_types = load_data_from_csv(os.path.join(current_dir, 'data/device_types.csv'), {
        'name': 'name',
        'view_config': 'view_config'
    })
    from plugins.models.device_type import DeviceTypeModel
    for device_type in device_types:
        data = DeviceTypeModel(name=device_type.get('name'), view_config=device_type.get('view_config'))
        merge_data(model=DeviceTypeModel, data=data, is_exist=DeviceTypeModel.name == data.name, session=session)


def create_default_users(factory):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    default_users = load_data_from_csv(os.path.join(
        current_dir,
        f'data/{factory}/default_users.csv' if factory else 'data/default_users.csv'
    ), {
        'username': 'username',
        'email': 'email',
        'lastname': 'lastname',
        'firstname': 'firstname',
        'password': 'password',
        'role': 'role'
    })
    from airflow.www.app import cached_app
    appbuilder = cached_app().appbuilder
    for user in default_users:
        try:
            role = appbuilder.sm.find_role(user['role'])
            if not role:
                raise Exception('{} is not a valid role.'.format(user['role']))
            user_created = appbuilder.sm.add_user(
                user['username'],
                user['firstname'],
                user['lastname'],
                user['email'],
                role,
                user['password'])
            if user_created:
                log.info('{} user {} created.'.format(user['role'], user['username']))
            else:
                raise Exception('Failed to create user.')
        except Exception as e:
            log.error(e)


@provide_session
def create_default_connection(session=None):
    from airflow.utils import db
    from airflow.models import Connection

    db.merge_conn(
        Connection(
            conn_id='qcos_rabbitmq', conn_type='rabbitmq',
            login='admin',
            password='admin',
            schema='amqp',
            extra=json.dumps({
                'vhost': '/',
                'heartbeat': '0',
                'exchange': ''
            }),
            host='172.17.0.1', port=5672), session)

    db.merge_conn(
        Connection(
            conn_id='qcos_kafka', conn_type='kafka',
            login='admin',
            password='admin',
            schema='qcos_{}'.format(os.environ.get('FACTORY_CODE', '')),  # topic
            host='localhost:9092',  # bootstrap_servers, 服务器或者服务器列表(cluster)
            extra=json.dumps({
                # 为空会创建失败
                'group_id': 'qcos_{}'.format(os.environ.get('FACTORY_CODE', '')),
                'security_protocol': 'SSL_PLAINTEXT',
                'auth_type': 'SCRAM-SHA-256',
            })
        ), session)

    db.merge_conn(
        Connection(
            conn_id='qcos_redis', conn_type='redis',
            host='172.17.0.1', port=6379,
            extra='{"db": 0}'), session)

    db.merge_conn(
        Connection(
            conn_id='qcos_minio', conn_type='http',
            host='172.17.0.1', port=9000
        ), session)

    db.merge_conn(
        Connection(
            conn_id='qcos_report', conn_type='http',
            host='172.17.0.1', port=8686
        ), session)

    db.merge_conn(
        Connection(
            conn_id='cas_analysis', conn_type='http',
            host='127.0.0.1', port=9095
        ), session)

    db.merge_conn(
        Connection(
            conn_id='cas_training', conn_type='http',
            host='127.0.0.1', port=9095
        ), session)

    db.merge_conn(
        Connection(
            conn_id='cas_server', conn_type='http',
            host='127.0.0.1', port=9095
        ), session)


# Defining the plugin class
class LoadDefaultDataPlugin(AirflowPlugin):
    name = "load_default_data_plugin"

    @classmethod
    def on_load(cls, *args, **kwargs):

        try:
            create_default_connection()
        except Exception as e:
            log.error(e)

        try:
            if conf.getboolean('core', 'LOAD_DEFAULT_ERROR_TAG', fallback=True):
                create_default_error_tags()
        except Exception as e:
            log.error(e)

        try:
            if conf.getboolean('core', 'LOAD_DEFAULT_DEVICE_TYPE', fallback=True):
                create_device_type_support()
        except Exception as e:
            log.error(e)

        factory_code = get_factory_code()
        try:
            create_default_users(factory_code)
        except Exception as e:
            log.error(e)

        try:
            load_default_controller(factory_code)
        except Exception as e:
            log.error(e)
