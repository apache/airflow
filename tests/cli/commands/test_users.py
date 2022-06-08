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

import json
import os
import tempfile

import pytest
from click.testing import CliRunner

from airflow.cli.commands import users
from tests.test_utils.api_connexion_utils import delete_users

TEST_USER1_EMAIL = 'test-user1@example.com'
TEST_USER2_EMAIL = 'test-user2@example.com'
TEST_USER3_EMAIL = 'test-user3@example.com'


def _does_user_belong_to_role(appbuilder, email, rolename):
    user = appbuilder.sm.find_user(email=email)
    role = appbuilder.sm.find_role(rolename)
    if user and role:
        return role in user.roles

    return False


class TestCliUsers:
    @classmethod
    def setup_class(cls):
        cls.runner = CliRunner()

    @pytest.fixture(autouse=True)
    def _set_attrs(self, app, dagbag, parser):
        self.app = app
        self.dagbag = dagbag
        self.parser = parser
        self.appbuilder = self.app.appbuilder
        delete_users(app)
        yield
        delete_users(app)

    def test_cli_create_user_random_password(self):
        response = self.runner.invoke(
            users.create,
            [
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
            ],
        )

        assert response.exit_code == 0

    def test_cli_create_user_supplied_password(self):
        response = self.runner.invoke(
            users.create,
            [
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
            ],
        )

        assert response.exit_code == 0

    def test_cli_create_user_typed_password(self):
        response = self.runner.invoke(
            users.create,
            [
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
            ],
            input="testpw\ntestpw\n",
        )

        assert response.exit_code == 0

    def test_cli_create_user_mistyped_confirm_password(self):
        response = self.runner.invoke(
            users.create,
            [
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
            ],
            input="testpw\ntestp\n",
        )

        assert response.exit_code == 0

    def test_cli_create_user_random_and_supplied_password(self):
        response = self.runner.invoke(
            users.create,
            [
                '--username',
                'thisusershouldntexist',
                '--lastname',
                'shouldntexist',
                '--firstname',
                'thisuser',
                '--email',
                'thisusershouldntexist@example.com',
                '--role',
                'Viewer',
                '--password',
                'test',
                '--use-random-password',
            ],
        )

        assert response.exit_code != 0
        assert "cannot specify both" in response.stdout

    def test_cli_delete_user(self):
        response = self.runner.invoke(
            users.create,
            [
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
            ],
        )

        response = self.runner.invoke(
            users.delete,
            [
                '--username',
                'test3',
            ],
        )

        assert response.exit_code == 0
        assert 'User "test3" deleted' in response.stdout

    def test_cli_delete_user_by_email(self):
        self.runner.invoke(
            users.create,
            [
                '--username',
                'test4',
                '--lastname',
                'doe',
                '--firstname',
                'jon',
                '--email',
                'jdoe2@example.com',
                '--role',
                'Viewer',
                '--use-random-password',
            ],
        )

        response = self.runner.invoke(
            users.delete,
            [
                '--email',
                'jdoe2@example.com',
            ],
        )

        assert response.exit_code == 0
        assert 'User "test4" deleted' in response.stdout

    @pytest.mark.parametrize(
        'args, raise_match',
        [
            (
                [],
                'Missing args: must supply one of --username or --email',
            ),
            (
                [
                    'test_user_name99',
                    'jdoe2@example.com',
                ],
                'Conflicting args: must supply either --username or --email, but not both',
            ),
            (
                [
                    'test_user_name99',
                ],
                'User "test_user_name99" does not exist',
            ),
            (
                [
                    'jode2@example.com',
                ],
                'User "jode2@example.com" does not exist',
            ),
        ],
    )
    def test_find_user_exceptions(self, args, raise_match):
        with pytest.raises(
            SystemExit,
            match=raise_match,
        ):
            users._find_user(*args)

    def test_cli_list_users(self):
        for i in range(0, 3):
            self.runner.invoke(
                users.create,
                [
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
                ],
            )

        response = self.runner.invoke(users.list_)

        assert response.exit_code == 0

        for i in range(0, 3):
            assert f'user{i}' in response.stdout

    def test_cli_list_users_with_args(self):
        response = self.runner.invoke(users.list_, ['--output', 'json'])

        assert response.exit_code == 0

    def test_cli_import_users(self):
        def assert_user_in_roles(email, roles):
            for role in roles:
                assert _does_user_belong_to_role(self.appbuilder, email, role)

        def assert_user_not_in_roles(email, roles):
            for role in roles:
                assert not _does_user_belong_to_role(self.appbuilder, email, role)

        assert_user_not_in_roles(TEST_USER1_EMAIL, ['Admin', 'Op'])
        assert_user_not_in_roles(TEST_USER2_EMAIL, ['Public'])
        users_ = [
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
        self._import_users_from_file(users_)

        assert_user_in_roles(TEST_USER1_EMAIL, ['Admin', 'Op'])
        assert_user_in_roles(TEST_USER2_EMAIL, ['Public'])

        users_ = [
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
        self._import_users_from_file(users_)

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
        with open(users_filename) as file:
            retrieved_users = json.loads(file.read())
        os.remove(users_filename)

        # ensure that an export can be imported
        self._import_users_from_file(retrieved_users)

        def find_by_username(username):
            matches = [u for u in retrieved_users if u['username'] == username]
            assert matches, (
                f"Couldn't find user with username {username} in: "
                f"[{', '.join([u['username'] for u in retrieved_users])}]"
            )
            matches[0].pop('id')  # this key not required for import
            return matches[0]

        assert find_by_username('imported_user1') == user1
        assert find_by_username('imported_user2') == user2

    def _import_users_from_file(self, user_list):
        json_file_content = json.dumps(user_list)
        with tempfile.NamedTemporaryFile(delete=False) as f:
            try:
                f.write(json_file_content.encode())
                f.flush()

                response = self.runner.invoke(users.import_, [f.name])
            finally:
                os.remove(f.name)
            return response

    def _export_users_to_file(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            self.runner.invoke(users.export, [f.name])
            return f.name

    @pytest.fixture()
    def create_user_test4(self):
        self.runner.invoke(
            users.create,
            [
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
            ],
        )

    def test_cli_add_user_role(self, create_user_test4):
        assert not _does_user_belong_to_role(
            appbuilder=self.appbuilder, email=TEST_USER1_EMAIL, rolename='Op'
        ), "User should not yet be a member of role 'Op'"

        response = self.runner.invoke(users.add_role, ['--username', 'test4', '--role', 'Op'])

        assert response.exit_code == 0
        assert 'User "test4" added to role "Op"' in response.stdout

        assert _does_user_belong_to_role(
            appbuilder=self.appbuilder, email=TEST_USER1_EMAIL, rolename='Op'
        ), "User should have been added to role 'Op'"

    def test_cli_remove_user_role(self, create_user_test4):
        assert _does_user_belong_to_role(
            appbuilder=self.appbuilder, email=TEST_USER1_EMAIL, rolename='Viewer'
        ), "User should have been created with role 'Viewer'"

        response = self.runner.invoke(users.remove_role, ['--username', 'test4', '--role', 'Viewer'])

        assert response.exit_code == 0
        assert 'User "test4" removed from role "Viewer"' in response.stdout

        assert not _does_user_belong_to_role(
            appbuilder=self.appbuilder, email=TEST_USER1_EMAIL, rolename='Viewer'
        ), "User should have been removed from role 'Viewer'"

    @pytest.mark.parametrize(
        "action, role, message",
        [
            ["add", "Viewer", 'User "test4" is already a member of role "Viewer"'],
            ["add", "Foo", '"Foo" is not a valid role. Valid roles are'],
            ["remove", "Admin", 'User "test4" is not a member of role "Admin"'],
            ["remove", "Foo", '"Foo" is not a valid role. Valid roles are'],
        ],
    )
    def test_cli_manage_roles_exceptions(self, create_user_test4, action, role, message):
        args = ['--username', 'test4', '--role', role]
        if action == 'add':
            response = self.runner.invoke(users.add_role, args)
        else:
            response = self.runner.invoke(users.remove_role, args)

        assert response.exit_code != 0
        assert message in response.stdout

    @pytest.mark.parametrize(
        "user, message",
        [
            [
                {
                    "username": "imported_user1",
                    "lastname": "doe1",
                    "firstname": "john",
                    "email": TEST_USER1_EMAIL,
                    "roles": "This is not a list",
                },
                "Error: Input file didn't pass validation. See below:\n"
                "[Item 0]\n"
                "\troles: ['Not a valid list.']",
            ],
            [
                {
                    "username": "imported_user2",
                    "lastname": "doe2",
                    "firstname": "jon",
                    "email": TEST_USER2_EMAIL,
                    "roles": [],
                },
                "Error: Input file didn't pass validation. See below:\n"
                "[Item 0]\n"
                "\troles: ['Shorter than minimum length 1.']",
            ],
            [
                {
                    "username1": "imported_user3",
                    "lastname": "doe3",
                    "firstname": "jon",
                    "email": TEST_USER3_EMAIL,
                    "roles": ["Test"],
                },
                "Error: Input file didn't pass validation. See below:\n"
                "[Item 0]\n"
                "\tusername: ['Missing data for required field.']\n"
                "\tusername1: ['Unknown field.']",
            ],
            [
                "Wrong input",
                "Error: Input file didn't pass validation. See below:\n"
                "[Item 0]\n"
                "\t_schema: ['Invalid input type.']",
            ],
        ],
        ids=["Incorrect roles", "Empty roles", "Required field is missing", "Wrong input"],
    )
    def test_cli_import_users_exceptions(self, user, message):
        response = self._import_users_from_file([user])

        assert response.exit_code != 0
        assert message in response.stdout
