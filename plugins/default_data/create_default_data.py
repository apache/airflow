from airflow.utils.db import provide_session
import os
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.load_data_from_csv import load_data_from_csv


log = LoggingMixin().log


@provide_session
def load_default_controller(session=None):
    log.info("Loading default controllers")
    from airflow.models import TighteningController
    current_dir = os.path.dirname(os.path.abspath(__file__))
    val = load_data_from_csv(os.path.join(current_dir, 'default_controllers.csv'), {
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


default_error_tags = {
    '1': '曲线异常（未知原因）',
    '101': '螺栓粘滑',
    '102': '扭矩异常下落',
    '103': '重复拧紧',
    '104': '提前松手',
    '105': '角度过大',
    # '100': '提前松手',
    # '101': '螺栓放偏',
    # '102': '螺栓空转，没办法旋入',
    # '103': '螺栓被错误的预拧紧',
    # '104': '螺纹胶涂胶识别有无',
    # '105': '螺纹胶涂覆位置错误-前后',
    # '106': '螺钉太长',
    # '107': '螺钉太短',
    # '108': '工件开裂',
    # '109': '尖叫螺栓',
    # '110': '提前进入屈服阶段',
    # '111': '转角法监控扭矩小于下限值',
    # '112': '转角法监控扭矩大于上限值-或者临界上限值'
}


@provide_session
def merge_data(model, data, is_exist, session=None):
    if not session.query(model).filter(is_exist).first():
        session.add(data)
        session.commit()


@provide_session
def create_default_error_tags(session=None):
    from airflow.models import ErrorTag
    for key, value in default_error_tags.items():
        data = ErrorTag(lable=value, value=key)
        merge_data(model=ErrorTag, data=data, is_exist=ErrorTag.label == data.label, session=session)


support_device_types = {
    '拧紧工具': '{}',
    '压机': '{}',
}


@provide_session
def create_device_type_support(session=None):
    if not session:
        return
    from airflow.models.tightening_controller import DeviceTypeModel
    for name, view_config in support_device_types.items():
        data = DeviceTypeModel(name=name, view_config=view_config)
        merge_data(model=DeviceTypeModel, data=data, is_exist=DeviceTypeModel.name == data.name, session=session)


def create_default_users():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    default_users = load_data_from_csv(os.path.join(current_dir, 'default_users.csv'), {
        'username': 'username',
        'email': 'email',
        'lastname': 'lastname',
        'firstname': 'firstname',
        'password': 'password',
        'role': 'role'
    })
    from airflow.www_rbac.app import cached_appbuilder
    appbuilder = cached_appbuilder()
    for user in default_users:
        try:
            role = appbuilder.sm.find_role(user['role'])
            if not role:
                raise SystemExit('{} is not a valid role.'.format(user['role']))
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
                raise SystemExit('Failed to create user.')
        except Exception as e:
            log.error(e)

# Defining the plugin class
class LoadDefaultDataPlugin(AirflowPlugin):
    name = "load_default_data_plugin"

    @classmethod
    def on_load(cls, *args, **kwargs):
        create_default_users()
        load_default_controller()
        create_device_type_support()
        create_default_error_tags()
