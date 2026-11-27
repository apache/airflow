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

from importlib.metadata import PackageNotFoundError
from unittest import mock

import pytest


def test_decorators_plugin_import():
    """Test that decorators plugin can be imported."""
    from airflow_mypy.plugins import decorators

    assert hasattr(decorators, "plugin")
    assert callable(decorators.plugin)


def test_outputs_plugin_import():
    """Test that outputs plugin can be imported."""
    from airflow_mypy.plugins import outputs

    assert hasattr(outputs, "plugin")
    assert callable(outputs.plugin)


def test_decorators_plugin_entrypoint():
    """Test that decorators plugin entry point works."""
    from airflow_mypy.plugins.decorators import plugin

    # The plugin function should return a plugin class when called with a version string
    plugin_class = plugin("1.0")
    assert plugin_class is not None


def test_outputs_plugin_entrypoint():
    """Test that outputs plugin entry point works."""
    from airflow_mypy.plugins.outputs import plugin

    # The plugin function should return a plugin class when called with a version string
    plugin_class = plugin("1.0")
    assert plugin_class is not None


@pytest.mark.parametrize(
    ("installed", "always_true", "always_false"),
    [
        pytest.param("2.0.57", [], ["SQLALCHEMY_2_1"], id="2.0"),
        pytest.param("2.1.8", ["SQLALCHEMY_2_1"], [], id="2.1"),
        pytest.param("2.2.0b1", ["SQLALCHEMY_2_1"], [], id="2.2-prerelease"),
    ],
)
@mock.patch("airflow_mypy.plugins.sqlalchemy_version.version", autospec=True)
def test_sqlalchemy_version_plugin_sets_constant(mock_version, installed, always_true, always_false):
    from mypy.options import Options

    from airflow_mypy.plugins.sqlalchemy_version import plugin

    mock_version.return_value = installed
    options = Options()
    plugin("1.0")(options)
    assert options.always_true == always_true
    assert options.always_false == always_false


@mock.patch(
    "airflow_mypy.plugins.sqlalchemy_version.version",
    autospec=True,
    side_effect=PackageNotFoundError("sqlalchemy"),
)
def test_sqlalchemy_version_plugin_without_sqlalchemy(mock_version):
    from mypy.options import Options

    from airflow_mypy.plugins.sqlalchemy_version import plugin

    options = Options()
    plugin("1.0")(options)
    assert options.always_true == []
    assert options.always_false == []
