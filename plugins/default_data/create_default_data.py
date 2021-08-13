from airflow.utils.db import provide_session
import os
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.plugins_manager import AirflowPlugin
from plugins.utils.load_data_from_csv import load_data_from_csv
from airflow.configuration import conf
from plugins.factory_code.factory_code import get_factory_code

log = LoggingMixin().log


@provide_session
def load_default_controller(file_dir, session=None):
    log.info("Loading default controllers")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, 'data/{}/default_controllers.csv'.format(file_dir))
    if not os.path.exists(file_path):
        log.error("导入控制器目录不存在：{}".format(file_path))
        return
    from plugins.models.tightening_controller import TighteningController
    val = load_data_from_csv(file_path, {
        'controller_name': '控制器名称',
        'line_code': '工段编号',
        'work_center_code': '工位编号',
        'line_name': '工段名称',
        'work_center_name': '工位名称'
    })
    controllers = TighteningController.list_controllers(session=session)
    if len(controllers) > 0:
        log.info("Controllers already exists, skipping")
        return
    for controller in val:
        TighteningController.add_controller(
            controller_name=controller.get('controller_name', None),
            line_code=controller.get('line_code', None),
            work_center_code=controller.get('work_center_code', None),
            line_name=controller.get('line_name', None),
            work_center_name=controller.get('work_center_name', None),
            session=session
        )


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


# fixme: 该方法在1.10版本中不可用，2.x版本中可用，需要在升级后稍作修改
# def create_default_users():
#     current_dir = os.path.dirname(os.path.abspath(__file__))
#     default_users = load_data_from_csv(os.path.join(current_dir, 'data/nd/default_users.csv'), {
#         'username': 'username',
#         'email': 'email',
#         'lastname': 'lastname',
#         'firstname': 'firstname',
#         'password': 'password',
#         'role': 'role'
#     })
#     from airflow.www_rbac.app import cached_appbuilder
#     appbuilder = cached_appbuilder()
#     for user in default_users:
#         try:
#             role = appbuilder.sm.find_role(user['role'])
#             if not role:
#                 raise SystemExit('{} is not a valid role.'.format(user['role']))
#             user_created = appbuilder.sm.add_user(
#                 user['username'],
#                 user['firstname'],
#                 user['lastname'],
#                 user['email'],
#                 role,
#                 user['password'])
#             if user_created:
#                 log.info('{} user {} created.'.format(user['role'], user['username']))
#             else:
#                 raise SystemExit('Failed to create user.')
#         except Exception as e:
#             log.error(e)


# Defining the plugin class
class LoadDefaultDataPlugin(AirflowPlugin):
    name = "load_default_data_plugin"

    @classmethod
    def on_load(cls, *args, **kwargs):
        # create_default_users()
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

        try:
            factory_code = get_factory_code()
            load_default_controller(factory_code)
        except Exception as e:
            log.error(e)
