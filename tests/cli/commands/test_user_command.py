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

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout

from airflow import models
from airflow.cli import cli_parser
from airflow.cli.commands import user_command

TEST_USER1_EMAIL = 'test-user1@example.com'
TEST_USER2_EMAIL = 'test-user2@example.com'


def _does_user_belong_to_role(appbuilder, email, rolename):
    user = appbuilder.sm.find_user(email=email)
    role = appbuilder.sm.find_role(rolename)
    if user and role:
        return role in user.roles

    return False


class TestCliUsers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = models.DagBag(include_examples=True)
        cls.parser = cli_parser.get_parser()

    def setUp(self):
        from airflow.www import app as application

        self.app = application.create_app(testing=True)
        self.appbuilder = self.app.appbuilder  # pylint: disable=no-member
        self.clear_roles_and_roles()

    def tearDown(self):
        self.clear_roles_and_roles()

    def clear_roles_and_roles(self):
        for email in [TEST_USER1_EMAIL, TEST_USER2_EMAIL]:
            test_user = self.appbuilder.sm.find_user(email=email)
            if test_user:
                self.appbuilder.sm.del_register_user(test_user)
        for role_name in ['FakeTeamA', 'FakeTeamB']:
            if self.appbuilder.sm.find_role(role_name):
                self.appbuilder.sm.delete_role(role_name)

    def test_cli_create_user_random_password(self):
        args = self.parser.parse_args(
            [
                'users',
                'create',
                '--username',
                'test1',
                '--lastname',
                'doe',
                '--firstname',
                'jon',
                '--email',
                'jdoe@foo.com',
                '--role',
                'Viewer',
                '--use-random-password',
            ]
        )
        user_command.users_create(args)

    def test_cli_create_user_supplied_password(self):
        args = self.parser.parse_args(
            [
                'users',
                'create',
                '--username',
                'test2',
                '--lastname',
                'doe',
                '--firstname',
                'jon',
                '--email',
                'jdoe@apache.org',
                '--role',
                'Viewer',
                '--password',
                'test',
            ]
        )
        user_command.users_create(args)

    def test_cli_delete_user(self):
        args = self.parser.parse_args(
            [
                'users',
                'create',
                '--username',
                'test3',
                '--lastname',
                'doe',
                '--firstname',
                'jon',
                '--email',
                'jdoe@example.com',
                '--role',
                'Viewer',
                '--use-random-password',
            ]
        )
        user_command.users_create(args)
        args = self.parser.parse_args(
            [
                'users',
                'delete',
                '--username',
                'test3',
            ]
        )
        user_command.users_delete(args)

    def test_cli_list_users(self):
        for i in range(0, 3):
            args = self.parser.parse_args(
                [
                    'users',
                    'create',
                    '--username',
                    f'user{i}',
                    '--lastname',
                    'doe',
                    '--firstname',
                    'jon',
                    '--email',
                    f'jdoe+{i}@gmail.com',
                    '--role',
                    'Viewer',
                    '--use-random-password',
                ]
            )
            user_command.users_create(args)
        with redirect_stdout(io.StringIO()) as stdout:
            user_command.users_list(self.parser.parse_args(['users', 'list']))
            stdout = stdout.getvalue()
        for i in range(0, 3):
            self.assertIn(f'user{i}', stdout)

    def test_cli_list_users_with_args(self):
        user_command.users_list(self.parser.parse_args(['users', 'list', '--output', 'tsv']))

    def test_cli_import_users(self):
        def assert_user_in_roles(email, roles):
            for role in roles:
                self.assertTrue(_does_user_belong_to_role(self.appbuilder, email, role))

        def assert_user_not_in_roles(email, roles):
            for role in roles:
                self.assertFalse(_does_user_belong_to_role(self.appbuilder, email, role))

        assert_user_not_in_roles(TEST_USER1_EMAIL, ['Admin', 'Op'])
        assert_user_not_in_roles(TEST_USER2_EMAIL, ['Public'])
        users = [
            {
                "username": "imported_user1",
                "lastname": "doe1",
                "firstname": "jon",
                "email": TEST_USER1_EMAIL,
                "roles": ["Admin", "Op"],
            },
            {
                "username": "imported_user2",
                "lastname": "doe2",
                "firstname": "jon",
                "email": TEST_USER2_EMAIL,
                "roles": ["Public"],
            },
        ]
        self._import_users_from_file(users)

        assert_user_in_roles(TEST_USER1_EMAIL, ['Admin', 'Op'])
        assert_user_in_roles(TEST_USER2_EMAIL, ['Public'])

        users = [
            {
                "username": "imported_user1",
                "lastname": "doe1",
                "firstname": "jon",
                "email": TEST_USER1_EMAIL,
                "roles": ["Public"],
            },
            {
                "username": "imported_user2",
                "lastname": "doe2",
                "firstname": "jon",
                "email": TEST_USER2_EMAIL,
                "roles": ["Admin"],
            },
        ]
        self._import_users_from_file(users)

        assert_user_not_in_roles(TEST_USER1_EMAIL, ['Admin', 'Op'])
        assert_user_in_roles(TEST_USER1_EMAIL, ['Public'])
        assert_user_not_in_roles(TEST_USER2_EMAIL, ['Public'])
        assert_user_in_roles(TEST_USER2_EMAIL, ['Admin'])

    def test_cli_export_users(self):
        user1 = {
            "username": "imported_user1",
            "lastname": "doe1",
            "firstname": "jon",
            "email": TEST_USER1_EMAIL,
            "roles": ["Public"],
        }
        user2 = {
            "username": "imported_user2",
            "lastname": "doe2",
            "firstname": "jon",
            "email": TEST_USER2_EMAIL,
            "roles": ["Admin"],
        }
        self._import_users_from_file([user1, user2])

        users_filename = self._export_users_to_file()
        with open(users_filename, mode='r') as file:
            retrieved_users = json.loads(file.read())
        os.remove(users_filename)

        # ensure that an export can be imported
        self._import_users_from_file(retrieved_users)

        def find_by_username(username):
            matches = [u for u in retrieved_users if u['username'] == username]
            if not matches:
                self.fail(f"Couldn't find user with username {username}")
            matches[0].pop('id')  # this key not required for import
            return matches[0]

        self.assertEqual(find_by_username('imported_user1'), user1)
        self.assertEqual(find_by_username('imported_user2'), user2)

    def _import_users_from_file(self, user_list):
        json_file_content = json.dumps(user_list)
        f = tempfile.NamedTemporaryFile(delete=False)
        try:
            f.write(json_file_content.encode())
            f.flush()

            args = self.parser.parse_args(['users', 'import', f.name])
            user_command.users_import(args)
        finally:
            os.remove(f.name)

    def _export_users_to_file(self):
        f = tempfile.NamedTemporaryFile(delete=False)
        args = self.parser.parse_args(['users', 'export', f.name])
        user_command.users_export(args)
        return f.name

    def test_cli_add_user_role(self):
        args = self.parser.parse_args(
            [
                'users',
                'create',
                '--username',
                'test4',
                '--lastname',
                'doe',
                '--firstname',
                'jon',
                '--email',
                TEST_USER1_EMAIL,
                '--role',
                'Viewer',
                '--use-random-password',
            ]
        )
        user_command.users_create(args)

        self.assertFalse(
            _does_user_belong_to_role(appbuilder=self.appbuilder, email=TEST_USER1_EMAIL, rolename='Op'),
            "User should not yet be a member of role 'Op'",
        )

        args = self.parser.parse_args(['users', 'add-role', '--username', 'test4', '--role', 'Op'])
        user_command.users_manage_role(args, remove=False)

        self.assertTrue(
            _does_user_belong_to_role(appbuilder=self.appbuilder, email=TEST_USER1_EMAIL, rolename='Op'),
            "User should have been added to role 'Op'",
        )

    def test_cli_remove_user_role(self):
        args = self.parser.parse_args(
            [
                'users',
                'create',
                '--username',
                'test4',
                '--lastname',
                'doe',
                '--firstname',
                'jon',
                '--email',
                TEST_USER1_EMAIL,
                '--role',
                'Viewer',
                '--use-random-password',
            ]
        )
        user_command.users_create(args)

        self.assertTrue(
            _does_user_belong_to_role(appbuilder=self.appbuilder, email=TEST_USER1_EMAIL, rolename='Viewer'),
            "User should have been created with role 'Viewer'",
        )

        args = self.parser.parse_args(['users', 'remove-role', '--username', 'test4', '--role', 'Viewer'])
        user_command.users_manage_role(args, remove=True)

        self.assertFalse(
            _does_user_belong_to_role(appbuilder=self.appbuilder, email=TEST_USER1_EMAIL, rolename='Viewer'),
            "User should have been removed from role 'Viewer'",
        )
