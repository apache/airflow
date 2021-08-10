import os
from airflow.utils.log.logging_mixin import LoggingMixin
from plugins.utils.load_data_from_csv import load_data_from_csv

log = LoggingMixin().log


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
