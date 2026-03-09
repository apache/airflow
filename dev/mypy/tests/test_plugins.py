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
