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

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

import airflow.providers.fab.www.auth as auth
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.models import Connection, Pool, Variable
from airflow.providers.fab.www import app as application

from tests_common.test_utils.config import conf_vars

mock_call = Mock()


@pytest.fixture
def app():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
        }
    ):
        yield application.create_app(enable_plugins=False)


@pytest.mark.parametrize(
    ("decorator_name", "is_authorized_method_name"),
    [
        ("has_access_configuration", "is_authorized_configuration"),
        ("has_access_asset", "is_authorized_asset"),
        ("has_access_view", "is_authorized_view"),
    ],
)
class TestHasAccessNoDetails:
    def setup_method(self):
        mock_call.reset_mock()

    def method_test(self):
        mock_call()
        return True

    @patch("airflow.providers.fab.www.auth.get_auth_manager")
    @patch("airflow.providers.fab.www.auth.get_fab_auth_manager")
    def test_has_access_no_details_when_authorized(
        self, _, mock_get_auth_manager, decorator_name, is_authorized_method_name
    ):
        auth_manager = Mock()
        is_authorized_method = Mock()
        is_authorized_method.return_value = True
        setattr(auth_manager, is_authorized_method_name, is_authorized_method)
        mock_get_auth_manager.return_value = auth_manager

        result = getattr(auth, decorator_name)("GET")(self.method_test)()

        mock_call.assert_called_once()
        assert result is True

    @patch("airflow.providers.fab.www.auth.get_auth_manager")
    @patch("airflow.providers.fab.www.auth.get_fab_auth_manager")
    @patch("airflow.providers.fab.www.auth.render_template")
    def test_has_access_no_details_when_no_permission(
        self, mock_render_template, _, mock_get_auth_manager, decorator_name, is_authorized_method_name
    ):
        auth_manager = Mock()
        is_authorized_method = Mock()
        is_authorized_method.return_value = False
        setattr(auth_manager, is_authorized_method_name, is_authorized_method)
        auth_manager.is_logged_in.return_value = True
        auth_manager.is_authorized_view.return_value = False
        mock_get_auth_manager.return_value = auth_manager

        getattr(auth, decorator_name)("GET")(self.method_test)()

        mock_call.assert_not_called()
        mock_render_template.assert_called_once()

    @pytest.mark.db_test
    @patch("airflow.providers.fab.www.auth.get_auth_manager")
    def test_has_access_no_details_when_not_logged_in(
        self, mock_get_auth_manager, app, decorator_name, is_authorized_method_name
    ):
        auth_manager = Mock()
        is_authorized_method = Mock()
        is_authorized_method.return_value = False
        setattr(auth_manager, is_authorized_method_name, is_authorized_method)
        auth_manager.is_logged_in.return_value = False
        auth_manager.get_url_login.return_value = "login_url"
        mock_get_auth_manager.return_value = auth_manager

        with app.test_request_context():
            result = getattr(auth, decorator_name)("GET")(self.method_test)()

        mock_call.assert_not_called()
        assert result.status_code == 302


@pytest.fixture
def get_connection():
    return [Connection("conn_1"), Connection("conn_2")]


@pytest.fixture
def get_pool():
    return [Pool(pool="pool_1"), Pool(pool="pool_2")]


@pytest.fixture
def get_variable():
    return [Variable("var_1"), Variable("var_2")]


@pytest.mark.parametrize(
    ("decorator_name", "is_authorized_method_name", "items"),
    [
        (
            "has_access_connection",
            "is_authorized_connection",
            "get_connection",
        ),
        ("has_access_pool", "is_authorized_pool", "get_pool"),
        ("has_access_variable", "is_authorized_variable", "get_variable"),
    ],
)
class TestHasAccessWithDetails:
    def setup_method(self):
        mock_call.reset_mock()

    def method_test(self, _view, arg):
        mock_call()
        return True

    @patch("airflow.providers.fab.www.auth.get_auth_manager")
    def test_has_access_with_details_when_authorized(
        self, mock_get_auth_manager, decorator_name, is_authorized_method_name, items, request
    ):
        items = request.getfixturevalue(items)
        auth_manager = Mock()
        is_authorized_method = Mock()
        is_authorized_method.return_value = True
        setattr(auth_manager, is_authorized_method_name, is_authorized_method)
        mock_get_auth_manager.return_value = auth_manager

        result = getattr(auth, decorator_name)("GET")(self.method_test)(None, items)

        mock_call.assert_called_once()
        assert result is True

    @pytest.mark.db_test
    @patch("airflow.providers.fab.www.auth.get_auth_manager")
    def test_has_access_with_details_when_unauthorized(
        self, mock_get_auth_manager, app, decorator_name, is_authorized_method_name, items, request
    ):
        items = request.getfixturevalue(items)
        auth_manager = Mock()
        is_authorized_method = Mock()
        is_authorized_method.return_value = False
        setattr(auth_manager, is_authorized_method_name, is_authorized_method)
        mock_get_auth_manager.return_value = auth_manager

        with app.test_request_context():
            result = getattr(auth, decorator_name)("GET")(self.method_test)(None, items)

        mock_call.assert_not_called()
        assert result.status_code == 302


@pytest.mark.parametrize(
    "dag_access_entity",
    [
        DagAccessEntity.XCOM,
        DagAccessEntity.RUN,
        DagAccessEntity.TASK_INSTANCE,
    ],
)
class TestHasAccessDagEntities:
    def setup_method(self):
        mock_call.reset_mock()

    def method_test(self, _view, arg):
        mock_call()
        return True

    @patch("airflow.providers.fab.www.auth.get_auth_manager")
    def test_has_access_dag_entities_when_authorized(self, mock_get_auth_manager, dag_access_entity):
        auth_manager = Mock()
        auth_manager.batch_is_authorized_dag.return_value = True
        mock_get_auth_manager.return_value = auth_manager
        items = [Mock(dag_id="dag_1"), Mock(dag_id="dag_2")]

        result = auth.has_access_dag_entities("GET", dag_access_entity)(self.method_test)(None, items)

        mock_call.assert_called_once()
        assert result is True

    @pytest.mark.db_test
    @patch("airflow.providers.fab.www.auth.get_auth_manager")
    @patch("airflow.providers.fab.www.auth.get_fab_auth_manager")
    def test_has_access_dag_entities_when_unauthorized(
        self, _, mock_get_auth_manager, app, dag_access_entity
    ):
        auth_manager = Mock()
        auth_manager.batch_is_authorized_dag.return_value = False
        mock_get_auth_manager.return_value = auth_manager
        items = [Mock(dag_id="dag_1"), Mock(dag_id="dag_2")]

        with app.test_request_context():
            result = auth.has_access_dag_entities("GET", dag_access_entity)(self.method_test)(None, items)

        mock_call.assert_not_called()
        assert result.headers["Location"] == "/"

    @pytest.mark.db_test
    @patch("airflow.providers.fab.www.auth.get_auth_manager")
    def test_has_access_dag_entities_when_logged_out(self, mock_get_auth_manager, app, dag_access_entity):
        auth_manager = Mock()
        auth_manager.batch_is_authorized_dag.return_value = False
        auth_manager.is_logged_in.return_value = False
        auth_manager.get_url_login.return_value = "login_url"
        mock_get_auth_manager.return_value = auth_manager
        items = [Mock(dag_id="dag_1"), Mock(dag_id="dag_2")]

        with app.test_request_context():
            result = auth.has_access_dag_entities("GET", dag_access_entity)(self.method_test)(None, items)

        mock_call.assert_not_called()
        assert result.headers["Location"] == "login_url"
