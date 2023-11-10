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

import airflow.www.auth as auth
from airflow.auth.managers.models.resource_details import DagAccessEntity
from airflow.exceptions import RemovedInAirflow3Warning
from airflow.models import Connection, Pool, Variable
from airflow.security import permissions
from airflow.settings import json
from airflow.www.auth import has_access
from tests.test_utils.api_connexion_utils import create_user_scope

mock_call = Mock()


class TestHasAccessDecorator:
    def test_has_access_decorator_raises_deprecation_warning(self):
        with pytest.warns(RemovedInAirflow3Warning):

            @has_access
            def test_function():
                pass


@pytest.mark.parametrize(
    "decorator_name, is_authorized_method_name",
    [
        ("has_access_cluster_activity", "is_authorized_cluster_activity"),
        ("has_access_configuration", "is_authorized_configuration"),
        ("has_access_dataset", "is_authorized_dataset"),
        ("has_access_view", "is_authorized_view"),
    ],
)
class TestHasAccessNoDetails:
    def setup_method(self):
        mock_call.reset_mock()

    def method_test(self):
        mock_call()
        return True

    @patch("airflow.www.auth.get_auth_manager")
    def test_has_access_no_details_when_authorized(
        self, mock_get_auth_manager, decorator_name, is_authorized_method_name
    ):
        auth_manager = Mock()
        is_authorized_method = Mock()
        is_authorized_method.return_value = True
        setattr(auth_manager, is_authorized_method_name, is_authorized_method)
        mock_get_auth_manager.return_value = auth_manager

        result = getattr(auth, decorator_name)("GET")(self.method_test)()

        mock_call.assert_called_once()
        assert result is True

    @patch("airflow.www.auth.get_auth_manager")
    @patch("airflow.www.auth.render_template")
    def test_has_access_no_details_when_no_permission(
        self, mock_render_template, mock_get_auth_manager, decorator_name, is_authorized_method_name
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
    @patch("airflow.www.auth.get_auth_manager")
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


@pytest.mark.parametrize(
    "decorator_name, is_authorized_method_name, items",
    [
        ("has_access_connection", "is_authorized_connection", [Connection("conn_1"), Connection("conn_2")]),
        ("has_access_pool", "is_authorized_pool", [Pool(pool="pool_1"), Pool(pool="pool_2")]),
        ("has_access_variable", "is_authorized_variable", [Variable("var_1"), Variable("var_2")]),
    ],
)
class TestHasAccessWithDetails:
    def setup_method(self):
        mock_call.reset_mock()

    def method_test(self, _view, arg):
        mock_call()
        return True

    @patch("airflow.www.auth.get_auth_manager")
    def test_has_access_with_details_when_authorized(
        self, mock_get_auth_manager, decorator_name, is_authorized_method_name, items
    ):
        auth_manager = Mock()
        is_authorized_method = Mock()
        is_authorized_method.return_value = True
        setattr(auth_manager, is_authorized_method_name, is_authorized_method)
        mock_get_auth_manager.return_value = auth_manager

        result = getattr(auth, decorator_name)("GET")(self.method_test)(None, items)

        mock_call.assert_called_once()
        assert result is True

    @pytest.mark.db_test
    @patch("airflow.www.auth.get_auth_manager")
    def test_has_access_with_details_when_unauthorized(
        self, mock_get_auth_manager, app, decorator_name, is_authorized_method_name, items
    ):
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
        DagAccessEntity.SLA_MISS,
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

    @patch("airflow.www.auth.get_auth_manager")
    def test_has_access_dag_entities_when_authorized(self, mock_get_auth_manager, dag_access_entity):
        auth_manager = Mock()
        auth_manager.is_authorized_dag.return_value = True
        mock_get_auth_manager.return_value = auth_manager
        items = [Mock(dag_id="dag_1"), Mock(dag_id="dag_2")]

        result = auth.has_access_dag_entities("GET", dag_access_entity)(self.method_test)(None, items)

        mock_call.assert_called_once()
        assert result is True

    @pytest.mark.db_test
    @patch("airflow.www.auth.get_auth_manager")
    def test_has_access_dag_entities_when_unauthorized(self, mock_get_auth_manager, app, dag_access_entity):
        auth_manager = Mock()
        auth_manager.is_authorized_dag.return_value = False
        mock_get_auth_manager.return_value = auth_manager
        items = [Mock(dag_id="dag_1"), Mock(dag_id="dag_2")]

        with app.test_request_context():
            result = auth.has_access_dag_entities("GET", dag_access_entity)(self.method_test)(None, items)

        mock_call.assert_not_called()
        assert result.status_code == 302


@pytest.mark.db_test
class TestHasAccessDagDecorator:
    @pytest.mark.parametrize(
        "dag_id_args, dag_id_kwargs, dag_id_form, dag_id_json, fail",
        [
            ("a", None, None, None, False),
            (None, "b", None, None, False),
            (None, None, "c", None, False),
            (None, None, None, "d", False),
            ("a", "a", None, None, False),
            ("a", "a", "a", None, False),
            ("a", "a", "a", "a", False),
            (None, "a", "a", "a", False),
            (None, None, "a", "a", False),
            ("a", None, None, "a", False),
            ("a", None, "a", None, False),
            ("a", None, "c", None, True),
            (None, "b", "c", None, True),
            (None, None, "c", "d", True),
            ("a", "b", "c", "d", True),
        ],
    )
    def test_dag_id_consistency(
        self,
        app,
        dag_id_args: str | None,
        dag_id_kwargs: str | None,
        dag_id_form: str | None,
        dag_id_json: str | None,
        fail: bool,
    ):
        with app.test_request_context() as mock_context:
            from airflow.www.auth import has_access_dag

            mock_context.request.args = {"dag_id": dag_id_args} if dag_id_args else {}
            kwargs = {"dag_id": dag_id_kwargs} if dag_id_kwargs else {}
            mock_context.request.form = {"dag_id": dag_id_form} if dag_id_form else {}
            if dag_id_json:
                mock_context.request._cached_data = json.dumps({"dag_id": dag_id_json})
                mock_context.request._parsed_content_type = ["application/json"]

            with create_user_scope(
                app,
                username="test-user",
                role_name="limited-role",
                permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG)],
            ) as user:
                with patch(
                    "airflow.auth.managers.fab.fab_auth_manager.FabAuthManager.get_user"
                ) as mock_get_user:
                    mock_get_user.return_value = user

                    @has_access_dag("GET")
                    def test_func(**kwargs):
                        return True

                    result = test_func(**kwargs)
                    if fail:
                        assert result[1] == 403
                    else:
                        assert result is True
