#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import unittest
from unittest import mock

from flask_appbuilder import SQLA, Model, expose, has_access
from flask_appbuilder.security.sqla import models as sqla_models
from flask_appbuilder.views import BaseView, ModelView
from sqlalchemy import Column, Date, Float, Integer, String

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models import DagModel
from airflow.security import permissions
from airflow.www import app as application
from airflow.www.utils import CustomSQLAInterface
from tests.test_utils import fab_utils
from tests.test_utils.db import clear_db_dags, clear_db_runs
from tests.test_utils.mock_security_manager import MockSecurityManager

READ_WRITE = {'can_read', 'can_edit'}
READ_ONLY = {'can_read'}

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')
logging.getLogger().setLevel(logging.DEBUG)
log = logging.getLogger(__name__)


class SomeModel(Model):
    id = Column(Integer, primary_key=True)
    field_string = Column(String(50), unique=True, nullable=False)
    field_integer = Column(Integer())
    field_float = Column(Float())
    field_date = Column(Date())

    def __repr__(self):
        return str(self.field_string)


class SomeModelView(ModelView):
    datamodel = CustomSQLAInterface(SomeModel)
    base_permissions = ['can_list', 'can_show', 'can_add', 'can_edit', 'can_delete']
    list_columns = ['field_string', 'field_integer', 'field_float', 'field_date']


class SomeBaseView(BaseView):
    route_base = ''

    @expose('/some_action')
    @has_access
    def some_action(self):
        return "action!"


class TestSecurity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        settings.configure_orm()
        cls.session = settings.Session
        cls.app = application.create_app(testing=True)
        cls.appbuilder = cls.app.appbuilder  # pylint: disable=no-member
        cls.app.config['WTF_CSRF_ENABLED'] = False
        cls.security_manager = cls.appbuilder.sm
        cls.delete_roles()

    def setUp(self):
        clear_db_runs()
        clear_db_dags()
        self.db = SQLA(self.app)
        self.appbuilder.add_view(SomeBaseView, "SomeBaseView", category="BaseViews")
        self.appbuilder.add_view(SomeModelView, "SomeModelView", category="ModelViews")

        log.debug("Complete setup!")

    @classmethod
    def delete_roles(cls):
        for role_name in ['team-a', 'MyRole1', 'MyRole5', 'Test_Role', 'MyRole3', 'MyRole2']:
            fab_utils.delete_role(cls.app, role_name)

    def expect_user_is_in_role(self, user, rolename):
        self.security_manager.init_role(rolename, [], [])
        role = self.security_manager.find_role(rolename)
        if not role:
            self.security_manager.add_role(rolename)
            role = self.security_manager.find_role(rolename)
        user.roles = [role]
        self.security_manager.update_user(user)

    def assert_user_has_dag_perms(self, perms, dag_id, user=None):
        for perm in perms:
            self.assertTrue(
                self._has_dag_perm(perm, dag_id, user),
                "User should have '{}' on DAG '{}'".format(perm, dag_id),
            )

    def assert_user_does_not_have_dag_perms(self, dag_id, perms, user=None):
        for perm in perms:
            self.assertFalse(
                self._has_dag_perm(perm, dag_id, user),
                "User should not have '{}' on DAG '{}'".format(perm, dag_id),
            )

    def _has_dag_perm(self, perm, dag_id, user):
        # if not user:
        #     user = self.user
        return self.security_manager.has_access(perm, self.security_manager.prefixed_dag_id(dag_id), user)

    def tearDown(self):
        clear_db_runs()
        clear_db_dags()
        self.appbuilder = None
        self.app = None
        self.db = None
        log.debug("Complete teardown!")

    def test_init_role_baseview(self):
        role_name = 'MyRole3'
        role_perms = ['can_some_action']
        role_vms = ['SomeBaseView']
        self.security_manager.init_role(role_name, role_vms, role_perms)
        role = self.appbuilder.sm.find_role(role_name)
        self.assertIsNotNone(role)
        self.assertEqual(len(role_perms), len(role.permissions))

    def test_init_role_modelview(self):
        role_name = 'MyRole2'
        role_perms = ['can_list', 'can_show', 'can_add', 'can_edit', 'can_delete']
        role_vms = ['SomeModelView']
        self.security_manager.init_role(role_name, role_vms, role_perms)
        role = self.appbuilder.sm.find_role(role_name)
        self.assertIsNotNone(role)
        self.assertEqual(len(role_perms), len(role.permissions))

    def test_update_and_verify_permission_role(self):
        role_name = 'Test_Role'
        self.security_manager.init_role(role_name, [], [])
        role = self.security_manager.find_role(role_name)

        perm = self.security_manager.find_permission_view_menu('can_edit', 'RoleModelView')
        self.security_manager.add_permission_role(role, perm)
        role_perms_len = len(role.permissions)

        self.security_manager.init_role(role_name, [], [])
        new_role_perms_len = len(role.permissions)

        self.assertEqual(role_perms_len, new_role_perms_len)

    def test_get_user_roles(self):
        user = mock.MagicMock()
        user.is_anonymous = False
        roles = self.appbuilder.sm.find_role('Admin')
        user.roles = roles
        self.assertEqual(self.security_manager.get_user_roles(user), roles)

    @mock.patch('airflow.www.security.AirflowSecurityManager.get_user_roles')
    def test_get_all_permissions_views(self, mock_get_user_roles):
        role_name = 'MyRole5'
        role_perm = 'can_some_action'
        role_vm = 'SomeBaseView'
        username = 'get_all_permissions_views'

        with self.app.app_context():
            user = fab_utils.create_user(self.app, username, role_name, permissions=[(role_perm, role_vm),],)
            role = user.roles[0]
            mock_get_user_roles.return_value = [role]

            self.assertEqual(self.security_manager.get_all_permissions_views(), {(role_perm, role_vm)})

            mock_get_user_roles.return_value = []
            self.assertEqual(len(self.security_manager.get_all_permissions_views()), 0)

    def test_get_accessible_dag_ids(self):
        role_name = 'MyRole1'
        permission_action = ['can_read']
        dag_id = 'dag_id'
        username = "ElUser"

        user = fab_utils.create_user(
            self.app,
            username,
            role_name,
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAGS),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAGS),
            ],
        )

        dag_model = DagModel(dag_id=dag_id, fileloc="/tmp/dag_.py", schedule_interval="2 2 * * *")
        self.session.add(dag_model)
        self.session.commit()

        self.security_manager.sync_perm_for_dag(  # type: ignore  # pylint: disable=no-member
            dag_id, access_control={role_name: permission_action}
        )

        self.assertEqual(self.security_manager.get_accessible_dag_ids(user), {'dag_id'})

    @mock.patch('airflow.www.security.AirflowSecurityManager._has_view_access')
    def test_has_access(self, mock_has_view_access):
        user = mock.MagicMock()
        user.is_anonymous = False
        mock_has_view_access.return_value = True
        self.assertTrue(self.security_manager.has_access('perm', 'view', user))

    def test_sync_perm_for_dag_creates_permissions_on_view_menus(self):
        test_dag_id = 'TEST_DAG'
        prefixed_test_dag_id = f'DAG:{test_dag_id}'
        self.security_manager.sync_perm_for_dag(test_dag_id, access_control=None)
        self.assertIsNotNone(
            self.security_manager.find_permission_view_menu('can_read', prefixed_test_dag_id)
        )
        self.assertIsNotNone(
            self.security_manager.find_permission_view_menu('can_edit', prefixed_test_dag_id)
        )

    @mock.patch('airflow.www.security.AirflowSecurityManager._has_perm')
    @mock.patch('airflow.www.security.AirflowSecurityManager._has_role')
    def test_has_all_dag_access(self, mock_has_role, mock_has_perm):
        mock_has_role.return_value = True
        self.assertTrue(self.security_manager.has_all_dags_access())

        mock_has_role.return_value = False
        mock_has_perm.return_value = False
        self.assertFalse(self.security_manager.has_all_dags_access())

        mock_has_perm.return_value = True
        self.assertTrue(self.security_manager.has_all_dags_access())

    def test_access_control_with_non_existent_role(self):
        with self.assertRaises(AirflowException) as context:
            self.security_manager.sync_perm_for_dag(
                dag_id='access-control-test',
                access_control={'this-role-does-not-exist': ['can_edit', 'can_read']},
            )
        self.assertIn("role does not exist", str(context.exception))

    def test_all_dag_access_doesnt_give_non_dag_access(self):
        username = 'dag_access_user'
        role_name = 'dag_access_role'
        with self.app.app_context():
            user = fab_utils.create_user(
                self.app,
                username,
                role_name,
                permissions=[
                    (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAGS),
                    (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAGS),
                ],
            )
            self.assertTrue(
                self.security_manager.has_access(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAGS, user)
            )
            self.assertFalse(self.security_manager.has_access(permissions.ACTION_CAN_READ, 'Task', user))

    def test_access_control_with_invalid_permission(self):
        invalid_permissions = [
            'can_varimport',  # a real permission, but not a member of DAG_PERMS
            'can_eat_pudding',  # clearly not a real permission
        ]
        username = "LaUser"
        user = fab_utils.create_user(self.app, username=username, role_name='team-a',)
        for permission in invalid_permissions:
            self.expect_user_is_in_role(user, rolename='team-a')
            with self.assertRaises(AirflowException) as context:
                self.security_manager.sync_perm_for_dag(
                    'access_control_test', access_control={'team-a': {permission}}
                )
            self.assertIn("invalid permissions", str(context.exception))

    def test_access_control_is_set_on_init(self):
        username = 'access_control_is_set_on_init'
        role_name = 'team-a'
        with self.app.app_context():
            user = fab_utils.create_user(self.app, username, role_name, permissions=[],)
            self.expect_user_is_in_role(user, rolename='team-a')
            self.security_manager.sync_perm_for_dag(
                'access_control_test', access_control={'team-a': ['can_edit', 'can_read']}
            )
            self.assert_user_has_dag_perms(
                perms=['can_edit', 'can_read'], dag_id='access_control_test', user=user
            )

            self.expect_user_is_in_role(user, rolename='NOT-team-a')
            self.assert_user_does_not_have_dag_perms(
                perms=['can_edit', 'can_read'], dag_id='access_control_test', user=user
            )

    def test_access_control_stale_perms_are_revoked(self):
        username = 'access_control_stale_perms_are_revoked'
        role_name = 'team-a'
        with self.app.app_context():
            user = fab_utils.create_user(self.app, username, role_name, permissions=[],)
            self.expect_user_is_in_role(user, rolename='team-a')
            self.security_manager.sync_perm_for_dag(
                'access_control_test', access_control={'team-a': READ_WRITE}
            )
            self.assert_user_has_dag_perms(perms=READ_WRITE, dag_id='access_control_test', user=user)

            self.security_manager.sync_perm_for_dag(
                'access_control_test', access_control={'team-a': READ_ONLY}
            )
            self.assert_user_has_dag_perms(perms=['can_read'], dag_id='access_control_test', user=user)
            self.assert_user_does_not_have_dag_perms(
                perms=['can_edit'], dag_id='access_control_test', user=user
            )

    def test_no_additional_dag_permission_views_created(self):
        ab_perm_view_role = sqla_models.assoc_permissionview_role

        self.security_manager.sync_roles()
        num_pv_before = self.db.session().query(ab_perm_view_role).count()
        self.security_manager.sync_roles()
        num_pv_after = self.db.session().query(ab_perm_view_role).count()
        self.assertEqual(num_pv_before, num_pv_after)

    def test_override_role_vm(self):
        test_security_manager = MockSecurityManager(appbuilder=self.appbuilder)
        self.assertEqual(len(test_security_manager.VIEWER_VMS), 1)
        self.assertEqual(test_security_manager.VIEWER_VMS, {'Airflow'})
