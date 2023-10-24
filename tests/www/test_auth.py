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

from unittest.mock import patch

import pytest

from airflow.security import permissions
from airflow.settings import json
from tests.test_utils.api_connexion_utils import create_user_scope
from tests.www.test_security import SomeBaseView, SomeModelView


@pytest.fixture(scope="module")
def app_builder(app):
    app_builder = app.appbuilder
    app_builder.add_view(SomeBaseView, "SomeBaseView", category="BaseViews")
    app_builder.add_view(SomeModelView, "SomeModelView", category="ModelViews")
    return app.appbuilder


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
            with patch("airflow.auth.managers.fab.fab_auth_manager.FabAuthManager.get_user") as mock_get_user:
                mock_get_user.return_value = user

                @has_access_dag("GET")
                def test_func(**kwargs):
                    return True

                result = test_func(**kwargs)
                if fail:
                    assert result[1] == 403
                else:
                    assert result is True
