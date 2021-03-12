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

from flask_appbuilder.security.sqla.models import User
from parameterized import parameterized

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.www import app
from tests.test_utils.config import conf_vars
from tests.test_utils.decorators import dont_initialize_flask_app_submodules

DEFAULT_TIME = "2020-06-11T18:00:00+00:00"


class TestUserEndpoint(unittest.TestCase):
    @classmethod
    @dont_initialize_flask_app_submodules(
        skip_all_except=["init_appbuilder", "init_api_experimental_auth", "init_api_connexion"]
    )
    def setUpClass(cls) -> None:
        super().setUpClass()
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            cls.app = app.create_app(testing=True)  # type:ignore
        cls.client = cls.app.test_client()  # type:ignore

    @provide_session
    def tearDown(self, session) -> None:
        self.delete_users(session)

    def _create_users(self, count, roles=None):
        # create users with defined created_on and changed_on date
        # for easy testing
        if roles is None:
            roles = []
        return [
            User(
                first_name=f'test{i}',
                last_name=f'test{i}',
                username=f'TEST_USER{i}',
                email=f'mytest@test{i}.org',
                roles=roles or [],
                created_on=timezone.parse(DEFAULT_TIME),
                changed_on=timezone.parse(DEFAULT_TIME),
            )
            for i in range(1, count + 1)
        ]

    @provide_session
    def delete_users(self, session):
        # Delete users that have our custom default time
        users = session.query(User).filter(User.changed_on == timezone.parse(DEFAULT_TIME)).all()
        for user in users:
            session.delete(user)
        session.commit()


class TestGetUser(TestUserEndpoint):
    @provide_session
    def test_should_respond_200(self, session):
        users = self._create_users(1)
        session.add_all(users)
        session.commit()
        response = self.client.get("/api/v1/users/TEST_USER1")
        assert response.status_code == 200
        assert response.json == {
            'active': None,
            'changed_on': DEFAULT_TIME,
            'created_on': DEFAULT_TIME,
            'email': 'mytest@test1.org',
            'fail_login_count': None,
            'first_name': 'test1',
            'last_login': None,
            'last_name': 'test1',
            'login_count': None,
            'roles': [],
            'user_id': users[0].id,
            'username': 'TEST_USER1',
        }

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/users/invalid-user")
        assert response.status_code == 404
        assert {
            'detail': "The User with username `invalid-user` was not found",
            'status': 404,
            'title': 'User not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        } == response.json


class TestGetUsers(TestUserEndpoint):
    @provide_session
    def test_should_response_200(self, session):
        users = self._create_users(2)
        session.add_all(users)
        session.commit()
        response = self.client.get("/api/v1/users")
        assert response.status_code == 200
        assert response.json == {
            'total_entries': 2,
            'users': [
                {
                    'active': None,
                    'changed_on': '2020-06-11T18:00:00+00:00',
                    'created_on': '2020-06-11T18:00:00+00:00',
                    'email': 'mytest@test1.org',
                    'fail_login_count': None,
                    'first_name': 'test1',
                    'last_login': None,
                    'last_name': 'test1',
                    'login_count': None,
                    'roles': [],
                    'user_id': users[0].id,
                    'username': 'TEST_USER1',
                },
                {
                    'active': None,
                    'changed_on': '2020-06-11T18:00:00+00:00',
                    'created_on': '2020-06-11T18:00:00+00:00',
                    'email': 'mytest@test2.org',
                    'fail_login_count': None,
                    'first_name': 'test2',
                    'last_login': None,
                    'last_name': 'test2',
                    'login_count': None,
                    'roles': [],
                    'user_id': users[1].id,
                    'username': 'TEST_USER2',
                },
            ],
        }


class TestGetUsersPagination(TestUserEndpoint):
    @parameterized.expand(
        [
            ("/api/v1/users?limit=1", ['TEST_USER1']),
            ("/api/v1/users?limit=2", ['TEST_USER1', "TEST_USER2"]),
            (
                "/api/v1/users?offset=5",
                [
                    "TEST_USER6",
                    "TEST_USER7",
                    "TEST_USER8",
                    "TEST_USER9",
                    "TEST_USER10",
                ],
            ),
            (
                "/api/v1/users?offset=0",
                [
                    "TEST_USER1",
                    "TEST_USER2",
                    "TEST_USER3",
                    "TEST_USER4",
                    "TEST_USER5",
                    "TEST_USER6",
                    "TEST_USER7",
                    "TEST_USER8",
                    "TEST_USER9",
                    "TEST_USER10",
                ],
            ),
            ("/api/v1/users?limit=1&offset=5", ["TEST_USER6"]),
            ("/api/v1/users?limit=1&offset=1", ["TEST_USER2"]),
            (
                "/api/v1/users?limit=2&offset=2",
                ["TEST_USER3", "TEST_USER4"],
            ),
        ]
    )
    @provide_session
    def test_handle_limit_offset(self, url, expected_usernames, session):
        users = self._create_users(10)
        session.add_all(users)
        session.commit()
        response = self.client.get(url)
        assert response.status_code == 200
        assert response.json["total_entries"] == 10
        usernames = [user["username"] for user in response.json["users"] if user]
        assert usernames == expected_usernames

    @provide_session
    def test_should_respect_page_size_limit_default(self, session):
        users = self._create_users(200)
        session.add_all(users)
        session.commit()

        response = self.client.get("/api/v1/users")
        assert response.status_code == 200

        assert response.json["total_entries"] == 200
        assert len(response.json["users"]) == 100

    @provide_session
    def test_limit_of_zero_should_return_default(self, session):
        users = self._create_users(200)
        session.add_all(users)
        session.commit()

        response = self.client.get("/api/v1/users?limit=0")
        assert response.status_code == 200

        assert response.json["total_entries"] == 200
        assert len(response.json["users"]) == 100

    @provide_session
    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, session):
        users = self._create_users(200)
        session.add_all(users)
        session.commit()

        response = self.client.get("/api/v1/users?limit=180")
        assert response.status_code == 200
        assert len(response.json['users']) == 150
