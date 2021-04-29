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
from unittest import mock

import jinja2
import pytest

from airflow import settings
from airflow.www.app import create_app
from tests.test_utils.api_connexion_utils import delete_roles
from tests.test_utils.decorators import dont_initialize_flask_app_submodules


@pytest.fixture(autouse=True, scope="module")
def session():
    settings.configure_orm()
    yield settings.Session


@pytest.fixture(scope="module")
def app():
    @dont_initialize_flask_app_submodules(
        skip_all_except=[
            "init_appbuilder",
            "init_appbuilder_views",
            "init_flash_views",
            "init_jinja_globals",
        ]
    )
    def factory():
        return create_app(testing=True)

    app = factory()
    app.config["WTF_CSRF_ENABLED"] = False
    app.jinja_env.undefined = jinja2.StrictUndefined

    security_manager = app.appbuilder.sm  # pylint: disable=no-member

    patch_path = "flask_appbuilder.security.manager.check_password_hash"
    with mock.patch(patch_path) as check_password_hash:
        check_password_hash.return_value = True
        security_manager.add_user(
            username='test',
            first_name='test',
            last_name='test',
            email='test@fab.org',
            role=security_manager.find_role('Admin'),
            password='test',
        )
        security_manager.add_user(
            username='test_user',
            first_name='test_user',
            last_name='test_user',
            email='test_user@fab.org',
            role=security_manager.find_role('User'),
            password='test_user',
        )
        security_manager.add_user(
            username='test_viewer',
            first_name='test_viewer',
            last_name='test_viewer',
            email='test_viewer@fab.org',
            role=security_manager.find_role('Viewer'),
            password='test_viewer',
        )

    yield app

    delete_roles(app)


@pytest.fixture()
def admin_client(app):
    client = app.test_client()
    resp = client.post("/login/", data={"username": "test", "password": "test"})
    assert resp.status_code == 302
    yield client


class Checker:
    def check_content_in_response(self, text, resp, resp_code=200):
        resp_html = resp.data.decode('utf-8')
        assert resp_code == resp.status_code
        if isinstance(text, list):
            for line in text:
                assert line in resp_html
        else:
            assert text in resp_html

    def check_content_not_in_response(self, text, resp, resp_code=200):
        resp_html = resp.data.decode('utf-8')
        assert resp_code == resp.status_code
        if isinstance(text, list):
            for line in text:
                assert line not in resp_html
        else:
            assert text not in resp_html


@pytest.fixture(scope="session")
def checker():
    return Checker()
