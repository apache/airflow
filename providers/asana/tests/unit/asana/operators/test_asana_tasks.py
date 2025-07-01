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

from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.asana.operators.asana_tasks import (
    AsanaCreateTaskOperator,
    AsanaDeleteTaskOperator,
    AsanaFindTaskOperator,
    AsanaUpdateTaskOperator,
)


class TestAsanaTaskOperators:
    """
    Test that the AsanaTaskOperators are using the python-asana methods as expected.
    """

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(Connection(conn_id="asana_test", conn_type="asana", password="test"))
        create_connection_without_db(Connection(conn_id="asana_default", conn_type="asana", password="test"))

    @patch("airflow.providers.asana.operators.asana_tasks.AsanaHook")
    def test_asana_create_task_operator_with_default_conn(self, mock_asana_hook):
        """
        Tests that the AsanaCreateTaskOperator uses the default connection.
        """

        mock_hook_instance = MagicMock()
        mock_asana_hook.return_value = mock_hook_instance
        mock_hook_instance.create_task.return_value = {"gid": "1"}

        create_task = AsanaCreateTaskOperator(
            task_id="create_task",
            name="test",
            task_parameters={"workspace": "1"},
        )
        result = create_task.execute({})
        assert create_task.conn_id == "asana_default"
        mock_hook_instance.create_task.assert_called_once_with("test", {"workspace": "1"})
        assert result == "1"

    @patch("airflow.providers.asana.operators.asana_tasks.AsanaHook")
    def test_asana_create_task_operator(self, mock_asana_hook):
        """
        Tests that the AsanaCreateTaskOperator makes the expected call to python-asana given valid arguments.
        """

        mock_hook_instance = MagicMock()
        mock_asana_hook.return_value = mock_hook_instance
        mock_hook_instance.create_task.return_value = {"gid": "1"}

        create_task = AsanaCreateTaskOperator(
            task_id="create_task",
            conn_id="asana_test",
            name="test",
            task_parameters={"workspace": "1"},
        )
        result = create_task.execute({})
        mock_hook_instance.create_task.assert_called_once_with("test", {"workspace": "1"})
        assert result == "1"

    @patch("airflow.providers.asana.operators.asana_tasks.AsanaHook")
    def test_asana_find_task_operator_with_default_conn(self, mock_asana_hook):
        """
        Tests that the AsanaFindTaskOperator uses the default connection.
        """

        mock_hook_instance = MagicMock()
        mock_asana_hook.return_value = mock_hook_instance
        mock_hook_instance.find_task.return_value = {"gid": "1"}

        find_task = AsanaFindTaskOperator(
            task_id="find_task",
            search_parameters={"project": "test"},
        )
        assert find_task.conn_id == "asana_default"

        result = find_task.execute({})
        mock_hook_instance.find_task.assert_called_once_with({"project": "test"})
        assert result == {"gid": "1"}

    @patch("airflow.providers.asana.operators.asana_tasks.AsanaHook")
    def test_asana_find_task_operator(self, mock_asana_hook):
        """
        Tests that the AsanaFindTaskOperator makes the expected call to python-asana given valid arguments.
        """

        mock_hook_instance = MagicMock()
        mock_asana_hook.return_value = mock_hook_instance
        mock_hook_instance.find_task.return_value = {"gid": "1"}

        task = AsanaFindTaskOperator(
            task_id="find_task",
            conn_id="asana_test",
            search_parameters={"project": "test"},
        )
        result = task.execute({})
        mock_hook_instance.find_task.assert_called_once_with({"project": "test"})
        assert result == {"gid": "1"}

    @patch("airflow.providers.asana.operators.asana_tasks.AsanaHook")
    def test_asana_update_task_operator_default_conn(self, mock_asana_hook):
        """
        Tests that the AsanaUpdateTaskOperator uses the default connection.
        """

        mock_hook_instance = MagicMock()
        mock_asana_hook.return_value = mock_hook_instance
        update_task = AsanaUpdateTaskOperator(
            task_id="update_task",
            asana_task_gid="test",
            task_parameters={"completed": True},
        )
        assert update_task.conn_id == "asana_default"
        update_task.execute({})
        mock_hook_instance.update_task.assert_called_once_with("test", {"completed": True})

    @patch("airflow.providers.asana.operators.asana_tasks.AsanaHook")
    def test_asana_update_task_operator(self, mock_asana_hook):
        """
        Tests that the AsanaUpdateTaskOperator makes the expected call to python-asana given valid arguments.
        """
        mock_hook_instance = MagicMock()
        mock_asana_hook.return_value = mock_hook_instance
        update_task = AsanaUpdateTaskOperator(
            task_id="update_task",
            conn_id="asana_test",
            asana_task_gid="test",
            task_parameters={"completed": True},
        )
        update_task.execute({})
        mock_hook_instance.update_task.assert_called_once_with("test", {"completed": True})

    @patch("airflow.providers.asana.operators.asana_tasks.AsanaHook")
    def test_asana_delete_task_operator_with_default_conn(self, mock_asana_hook):
        """
        Tests that the AsanaDeleteTaskOperator uses the default connection.
        """

        mock_hook_instance = MagicMock()
        mock_asana_hook.return_value = mock_hook_instance

        delete_task = AsanaDeleteTaskOperator(
            task_id="delete_task",
            asana_task_gid="test",
        )
        delete_task.execute({})
        assert delete_task.conn_id == "asana_default"
        mock_hook_instance.delete_task.assert_called_once_with("test")

    @patch("airflow.providers.asana.operators.asana_tasks.AsanaHook")
    def test_asana_delete_task_operator(self, mock_asana_hook):
        """
        Tests that the AsanaDeleteTaskOperator makes the expected call to python-asana given valid arguments.
        """
        mock_hook_instance = MagicMock()
        mock_asana_hook.return_value = mock_hook_instance

        delete_task = AsanaDeleteTaskOperator(
            task_id="delete_task",
            conn_id="asana_test",
            asana_task_gid="test",
        )
        delete_task.execute({})
        mock_hook_instance.delete_task.assert_called_once_with("test")
