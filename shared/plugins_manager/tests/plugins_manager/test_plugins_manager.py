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
from __future__ import annotations

import contextlib
import logging
import sys
from unittest import mock

import pytest


@pytest.fixture
def mock_metadata_distribution(mocker):
    @contextlib.contextmanager
    def wrapper(*args, **kwargs):
        if sys.version_info < (3, 12):
            patch_fq = "importlib_metadata.distributions"
        else:
            patch_fq = "importlib.metadata.distributions"

        with mock.patch(patch_fq, *args, **kwargs) as m:
            yield m

    return wrapper


class TestPluginsDirectorySource:
    def test_should_return_correct_path_name(self):
        from airflow_shared.plugins_manager import plugins_manager

        source = plugins_manager.PluginsDirectorySource(__file__)
        assert source.path == "test_plugins_manager.py"
        assert str(source) == "$PLUGINS_FOLDER/test_plugins_manager.py"
        assert source.__html__() == "<em>$PLUGINS_FOLDER/</em>test_plugins_manager.py"


class TestEntryPointSource:
    def test_should_return_correct_source_details(self, mock_metadata_distribution):
        from airflow_shared.plugins_manager import plugins_manager

        mock_entrypoint = mock.Mock()
        mock_entrypoint.name = "test-entrypoint-plugin"
        mock_entrypoint.module = "module_name_plugin"

        mock_dist = mock.Mock()
        mock_dist.metadata = {"Name": "test-entrypoint-plugin"}
        mock_dist.version = "1.0.0"
        mock_dist.entry_points = [mock_entrypoint]

        with mock_metadata_distribution(return_value=[mock_dist]):
            plugins_manager._load_entrypoint_plugins()

        source = plugins_manager.EntryPointSource(mock_entrypoint, mock_dist)
        assert str(mock_entrypoint) == source.entrypoint
        assert "test-entrypoint-plugin==1.0.0: " + str(mock_entrypoint) == str(source)
        assert "<em>test-entrypoint-plugin==1.0.0:</em> " + str(mock_entrypoint) == source.__html__()


class TestPluginsManager:
    def test_entrypoint_plugin_errors_dont_raise_exceptions(self, mock_metadata_distribution, caplog):
        """
        Test that Airflow does not raise an error if there is any Exception because of a plugin.
        """
        from airflow_shared.plugins_manager import plugins_manager

        mock_dist = mock.Mock()
        mock_dist.metadata = {"Name": "test-dist"}

        mock_entrypoint = mock.Mock()
        mock_entrypoint.name = "test-entrypoint"
        mock_entrypoint.group = "airflow.plugins"
        mock_entrypoint.module = "test.plugins.test_plugins_manager"
        mock_entrypoint.load.side_effect = ImportError("my_fake_module not found")
        mock_dist.entry_points = [mock_entrypoint]

        with (
            mock_metadata_distribution(return_value=[mock_dist]),
            caplog.at_level(logging.ERROR, logger="airflow_shared.plugins_manager.plugins_manager"),
        ):
            _, import_errors = plugins_manager._load_entrypoint_plugins()

            received_logs = caplog.text
            # Assert Traceback is shown too
            assert "Traceback (most recent call last):" in received_logs
            assert "my_fake_module not found" in received_logs
            assert "Failed to import plugin test-entrypoint" in received_logs
            assert (
                "test.plugins.test_plugins_manager",
                "my_fake_module not found",
            ) in import_errors.items()
