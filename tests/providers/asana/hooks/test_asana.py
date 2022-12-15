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

import os
from unittest.mock import patch

import pytest
from asana import Client
from pytest import param

from airflow.models import Connection
from airflow.providers.asana.hooks.asana import AsanaHook
from tests.test_utils.providers import get_provider_min_airflow_version, object_exists


class TestAsanaHook:
    """
    Tests for AsanaHook Asana client retrieval
    """

    def test_asana_client_retrieved(self):
        """
        Test that we successfully retrieve an Asana client given a Connection with complete information.
        :return: None
        """
        with patch.object(
            AsanaHook, "get_connection", return_value=Connection(conn_type="asana", password="test")
        ):
            hook = AsanaHook()
        client = hook.get_conn()
        assert type(client) == Client

    def test_missing_password_raises(self):
        """
        Test that the Asana hook raises an exception if password not provided in connection.
        :return: None
        """
        with patch.object(AsanaHook, "get_connection", return_value=Connection(conn_type="asana")):
            hook = AsanaHook()
        with pytest.raises(ValueError):
            hook.get_conn()

    def test_merge_create_task_parameters_default_project(self):
        """
        Test that merge_create_task_parameters correctly merges the default and method parameters when we
        do not override the default project.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"extra__asana__project": "1"}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"name": "test", "projects": ["1"]}
        assert hook._merge_create_task_parameters("test", {}) == expected_merged_params

    def test_merge_create_task_parameters_specified_project(self):
        """
        Test that merge_create_task_parameters correctly merges the default and method parameters when we
        override the default project.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"extra__asana__project": "1"}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"name": "test", "projects": ["1", "2"]}
        assert hook._merge_create_task_parameters("test", {"projects": ["1", "2"]}) == expected_merged_params

    def test_merge_create_task_parameters_specified_workspace(self):
        """
        Test that merge_create_task_parameters correctly merges the default and method parameters when we
        do not override the default workspace.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"extra__asana__workspace": "1"}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"name": "test", "workspace": "1"}
        assert hook._merge_create_task_parameters("test", {}) == expected_merged_params

    def test_merge_create_task_parameters_default_project_overrides_default_workspace(self):
        """
        Test that merge_create_task_parameters uses the default project over the default workspace
        if it is available
        :return: None
        """
        conn = Connection(
            conn_type="asana",
            password="test",
            extra='{"extra__asana__workspace": "1", "extra__asana__project": "1"}',
        )
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"name": "test", "projects": ["1"]}
        assert hook._merge_create_task_parameters("test", {}) == expected_merged_params

    def test_merge_create_task_parameters_specified_project_overrides_default_workspace(self):
        """
        Test that merge_create_task_parameters uses the method parameter project over the default workspace
        if it is available
        :return: None
        """
        conn = Connection(
            conn_type="asana",
            password="test",
            extra='{"extra__asana__workspace": "1"}',
        )
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"name": "test", "projects": ["2"]}
        assert hook._merge_create_task_parameters("test", {"projects": ["2"]}) == expected_merged_params

    def test_merge_find_task_parameters_default_project(self):
        """
        Test that merge_find_task_parameters correctly merges the default and method parameters when we
        do not override the default project.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"extra__asana__project": "1"}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"project": "1"}
        assert hook._merge_find_task_parameters({}) == expected_merged_params

    def test_merge_find_task_parameters_specified_project(self):
        """
        Test that merge_find_task_parameters correctly merges the default and method parameters when we
        do override the default project.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"extra__asana__project": "1"}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"project": "2"}
        assert hook._merge_find_task_parameters({"project": "2"}) == expected_merged_params

    def test_merge_find_task_parameters_default_workspace(self):
        """
        Test that merge_find_task_parameters correctly merges the default and method parameters when we
        do not override the default workspace.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"extra__asana__workspace": "1"}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"workspace": "1", "assignee": "1"}
        assert hook._merge_find_task_parameters({"assignee": "1"}) == expected_merged_params

    def test_merge_find_task_parameters_specified_workspace(self):
        """
        Test that merge_find_task_parameters correctly merges the default and method parameters when we
        do override the default workspace.
        :return: None
        """
        conn = Connection(conn_type="asana", password="test", extra='{"extra__asana__workspace": "1"}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"workspace": "2", "assignee": "1"}
        assert hook._merge_find_task_parameters({"workspace": "2", "assignee": "1"}) == expected_merged_params

    def test_merge_find_task_parameters_default_project_overrides_workspace(self):
        """
        Test that merge_find_task_parameters uses the default project over the workspace if it is available
        :return: None
        """
        conn = Connection(
            conn_type="asana",
            password="test",
            extra='{"extra__asana__workspace": "1", "extra__asana__project": "1"}',
        )
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"project": "1"}
        assert hook._merge_find_task_parameters({}) == expected_merged_params

    def test_merge_find_task_parameters_specified_project_overrides_workspace(self):
        """
        Test that merge_find_task_parameters uses the method parameter project over the default workspace
        if it is available
        :return: None
        """
        conn = Connection(
            conn_type="asana",
            password="test",
            extra='{"extra__asana__workspace": "1"}',
        )
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"project": "2"}
        assert hook._merge_find_task_parameters({"project": "2"}) == expected_merged_params

    def test_merge_project_parameters(self):
        """
        Tests that default workspace is used if not overridden
        :return:
        """
        conn = Connection(conn_type="asana", password="test", extra='{"extra__asana__workspace": "1"}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"workspace": "1", "name": "name"}
        assert hook._merge_project_parameters({"name": "name"}) == expected_merged_params

    def test_merge_project_parameters_override(self):
        """
        Tests that default workspace is successfully overridden
        :return:
        """
        conn = Connection(conn_type="asana", password="test", extra='{"extra__asana__workspace": "1"}')
        with patch.object(AsanaHook, "get_connection", return_value=conn):
            hook = AsanaHook()
        expected_merged_params = {"workspace": "2"}
        assert hook._merge_project_parameters({"workspace": "2"}) == expected_merged_params

    def test__ensure_prefixes_removal(self):
        """Ensure that _ensure_prefixes is removed from snowflake when airflow min version >= 2.5.0."""
        path = "airflow.providers.asana.hooks.asana._ensure_prefixes"
        if not object_exists(path):
            raise Exception(
                "You must remove this test. It only exists to "
                "remind us to remove decorator `_ensure_prefixes`."
            )

        if get_provider_min_airflow_version("apache-airflow-providers-asana") >= (2, 5):
            raise Exception(
                "You must now remove `_ensure_prefixes` from AsanaHook."
                " The functionality is now taken care of by providers manager."
            )

    def test__ensure_prefixes(self):
        """
        Check that ensure_prefixes decorator working properly

        Note: remove this test when removing ensure_prefixes (after min airflow version >= 2.5.0
        """
        assert list(AsanaHook.get_ui_field_behaviour()["placeholders"].keys()) == [
            "password",
            "extra__asana__workspace",
            "extra__asana__project",
        ]

    @pytest.mark.parametrize(
        "uri",
        [
            param(
                "a://?extra__asana__workspace=abc&extra__asana__project=abc",
                id="prefix",
            ),
            param("a://?workspace=abc&project=abc", id="no-prefix"),
        ],
    )
    def test_backcompat_prefix_works(self, uri):
        with patch.dict(os.environ, {"AIRFLOW_CONN_MY_CONN": uri}):
            hook = AsanaHook("my_conn")
            assert hook.workspace == "abc"
            assert hook.project == "abc"

    def test_backcompat_prefix_both_prefers_short(self):
        with patch.dict(
            os.environ,
            {"AIRFLOW_CONN_MY_CONN": "a://?workspace=non-prefixed&extra__asana__workspace=prefixed"},
        ):
            hook = AsanaHook("my_conn")
            assert hook.workspace == "non-prefixed"
