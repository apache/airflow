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

from unittest import mock


@mock.patch("airflow.api_fastapi.app.init_dag_bag")
@mock.patch("airflow.api_fastapi.app.init_views")
@mock.patch("airflow.api_fastapi.app.init_plugins")
@mock.patch("airflow.api_fastapi.app.create_task_execution_api_app")
def test_core_api_app(
    mock_create_task_exec_api,
    mock_init_plugins,
    mock_init_views,
    mock_init_dag_bag,
    client,
):
    test_app = client(apps="core").app

    # Assert that core-related functions were called
    mock_init_dag_bag.assert_called_once_with(test_app)
    mock_init_views.assert_called_once_with(test_app)
    mock_init_plugins.assert_called_once_with(test_app)

    # Assert that execution-related functions were NOT called
    mock_create_task_exec_api.assert_not_called()


@mock.patch("airflow.api_fastapi.app.init_dag_bag")
@mock.patch("airflow.api_fastapi.app.init_views")
@mock.patch("airflow.api_fastapi.app.init_plugins")
@mock.patch("airflow.api_fastapi.app.create_task_execution_api_app")
def test_execution_api_app(
    mock_create_task_exec_api,
    mock_init_plugins,
    mock_init_views,
    mock_init_dag_bag,
    client,
):
    test_app = client(apps="execution").app

    # Assert that execution-related functions were called
    mock_create_task_exec_api.assert_called_once_with(test_app)

    # Assert that core-related functions were NOT called
    mock_init_dag_bag.assert_not_called()
    mock_init_views.assert_not_called()
    mock_init_plugins.assert_not_called()


@mock.patch("airflow.api_fastapi.app.init_dag_bag")
@mock.patch("airflow.api_fastapi.app.init_views")
@mock.patch("airflow.api_fastapi.app.init_plugins")
@mock.patch("airflow.api_fastapi.app.create_task_execution_api_app")
def test_all_apps(
    mock_create_task_exec_api,
    mock_init_plugins,
    mock_init_views,
    mock_init_dag_bag,
    client,
):
    test_app = client(apps="all").app

    # Assert that core-related functions were called
    mock_init_dag_bag.assert_called_once_with(test_app)
    mock_init_views.assert_called_once_with(test_app)
    mock_init_plugins.assert_called_once_with(test_app)

    # Assert that execution-related functions were also called
    mock_create_task_exec_api.assert_called_once_with(test_app)
