# -*- coding: utf-8 -*-
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

import unittest
import logging
import mock

from flask import Flask
from flask_appbuilder import AppBuilder, SQLA, Model, has_access, expose
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_appbuilder.security.sqla import models as sqla_models
from flask_appbuilder.views import ModelView, BaseView

from sqlalchemy import Column, Integer, String, Date, Float

from airflow.exceptions import AirflowException
from airflow.www.security import AirflowSecurityManager


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
    datamodel = SQLAInterface(SomeModel)
    base_permissions = ['can_list', 'can_show', 'can_add', 'can_edit', 'can_delete']
    list_columns = ['field_string', 'field_integer', 'field_float', 'field_date']


class SomeBaseView(BaseView):
    route_base = ''

    @expose('/some_action')
    @has_access
    def some_action(self):
        return "action!"


class TestSecurityManager(AirflowSecurityManager):
    VIEWER_VMS = {
        'Airflow',
    }


class TestSecurity(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///'
        self.app.config['SECRET_KEY'] = 'secret_key'
        self.app.config['CSRF_ENABLED'] = False
        self.app.config['WTF_CSRF_ENABLED'] = False
        self.db = SQLA(self.app)
        self.appbuilder = AppBuilder(self.app,
                                     self.db.session,
                                     security_manager_class=AirflowSecurityManager)
        self.security_manager = self.appbuilder.sm
        self.appbuilder.add_view(SomeBaseView, "SomeBaseView", category="BaseViews")
        self.appbuilder.add_view(SomeModelView, "SomeModelView", category="ModelViews")
        role_admin = self.security_manager.find_role('Admin')
        self.user = self.appbuilder.sm.add_user('admin', 'admin', 'user', 'admin@fab.org',
                                                role_admin, 'general')
        log.debug("Complete setup!")

    def expect_user_is_in_role(self, user, rolename):
        self.security_manager.init_role(rolename, [], [])
        role = self.security_manager.find_role(rolename)
        if not role:
            self.security_manager.add_role(rolename)
            role = self.security_manager.find_role(rolename)
        user.roles = [role]
        self.security_manager.update_user(user)

    def assert_user_has_dag_perms(self, perms, dag_id):
        for perm in perms:
            self.assertTrue(
                self._has_dag_perm(perm, dag_id),
                "User should have '{}' on DAG '{}'".format(perm, dag_id))

    def assert_user_does_not_have_dag_perms(self, dag_id, perms):
        for perm in perms:
            self.assertFalse(
                self._has_dag_perm(perm, dag_id),
                "User should not have '{}' on DAG '{}'".format(perm, dag_id))

    def _has_dag_perm(self, perm, dag_id):
        return self.security_manager.has_access(
            perm,
            dag_id,
            self.user)

    def tearDown(self):
        self.appbuilder = None
        self.app = None
        self.db = None
        log.debug("Complete teardown!")

    def test_init_role_baseview(self):
        role_name = 'MyRole1'
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

        perm = self.security_manager.\
            find_permission_view_menu('can_edit', 'RoleModelView')
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
        role_name = 'MyRole1'
        role_perms = ['can_some_action']
        role_vms = ['SomeBaseView']
        self.security_manager.init_role(role_name, role_vms, role_perms)
        role = self.security_manager.find_role(role_name)

        mock_get_user_roles.return_value = [role]
        self.assertEqual(self.security_manager
                         .get_all_permissions_views(),
                         {('can_some_action', 'SomeBaseView')})

        mock_get_user_roles.return_value = []
        self.assertEqual(len(self.security_manager
                             .get_all_permissions_views()), 0)

    @mock.patch('airflow.www.security.AirflowSecurityManager'
                '.get_all_permissions_views')
    @mock.patch('airflow.www.security.AirflowSecurityManager'
                '.get_user_roles')
    def test_get_accessible_dag_ids(self, mock_get_user_roles,
                                    mock_get_all_permissions_views):
        user = mock.MagicMock()
        role_name = 'MyRole1'
        role_perms = ['can_dag_read']
        role_vms = ['dag_id']
        self.security_manager.init_role(role_name, role_vms, role_perms)
        role = self.security_manager.find_role(role_name)
        user.roles = [role]
        user.is_anonymous = False
        mock_get_all_permissions_views.return_value = {('can_dag_read', 'dag_id')}

        mock_get_user_roles.return_value = [role]
        self.assertEqual(self.security_manager
                         .get_accessible_dag_ids(user), set(['dag_id']))

    @mock.patch('airflow.www.security.AirflowSecurityManager._has_view_access')
    def test_has_access(self, mock_has_view_access):
        user = mock.MagicMock()
        user.is_anonymous = False
        mock_has_view_access.return_value = True
        self.assertTrue(self.security_manager.has_access('perm', 'view', user))

    def test_sync_perm_for_dag_creates_permissions_on_view_menus(self):
        test_dag_id = 'TEST_DAG'
        self.security_manager.sync_perm_for_dag(test_dag_id, access_control=None)
        for dag_perm in self.security_manager.DAG_PERMS:
            self.assertIsNotNone(self.security_manager.
                                 find_permission_view_menu(dag_perm, test_dag_id))

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
                access_control={
                    'this-role-does-not-exist': ['can_dag_edit', 'can_dag_read']
                })
        self.assertIn("role does not exist", str(context.exception))

    def test_access_control_with_invalid_permission(self):
        invalid_permissions = [
            'can_varimport',  # a real permission, but not a member of DAG_PERMS
            'can_eat_pudding',  # clearly not a real permission
        ]
        for permission in invalid_permissions:
            self.expect_user_is_in_role(self.user, rolename='team-a')
            with self.assertRaises(AirflowException) as context:
                self.security_manager.sync_perm_for_dag(
                    'access_control_test',
                    access_control={
                        'team-a': {permission}
                    })
            self.assertIn("invalid permissions", str(context.exception))

    def test_access_control_is_set_on_init(self):
        self.expect_user_is_in_role(self.user, rolename='team-a')
        self.security_manager.sync_perm_for_dag(
            'access_control_test',
            access_control={
                'team-a': ['can_dag_edit', 'can_dag_read']
            })
        self.assert_user_has_dag_perms(
            perms=['can_dag_edit', 'can_dag_read'],
            dag_id='access_control_test',
        )

        self.expect_user_is_in_role(self.user, rolename='NOT-team-a')
        self.assert_user_does_not_have_dag_perms(
            perms=['can_dag_edit', 'can_dag_read'],
            dag_id='access_control_test',
        )

    def test_access_control_stale_perms_are_revoked(self):
        READ_WRITE = {'can_dag_read', 'can_dag_edit'}
        READ_ONLY = {'can_dag_read'}

        self.expect_user_is_in_role(self.user, rolename='team-a')
        self.security_manager.sync_perm_for_dag(
            'access_control_test',
            access_control={'team-a': READ_WRITE})
        self.assert_user_has_dag_perms(
            perms=READ_WRITE,
            dag_id='access_control_test',
        )

        self.security_manager.sync_perm_for_dag(
            'access_control_test',
            access_control={'team-a': READ_ONLY})
        self.assert_user_has_dag_perms(
            perms=['can_dag_read'],
            dag_id='access_control_test',
        )
        self.assert_user_does_not_have_dag_perms(
            perms=['can_dag_edit'],
            dag_id='access_control_test',
        )

    def test_no_additional_dag_permission_views_created(self):
        ab_perm_view_role = sqla_models.assoc_permissionview_role

        self.security_manager.sync_roles()
        num_pv_before = self.db.session().query(ab_perm_view_role).count()
        self.security_manager.sync_roles()
        num_pv_after = self.db.session().query(ab_perm_view_role).count()
        self.assertEqual(num_pv_before, num_pv_after)

    def test_override_role_vm(self):
        test_security_manager = TestSecurityManager(appbuilder=self.appbuilder)
        self.assertEqual(len(test_security_manager.VIEWER_VMS), 1)
        self.assertEqual(test_security_manager.VIEWER_VMS, {'Airflow'})
