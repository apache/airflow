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

from argparse import Namespace

import pluggy
import pytest

from airflow import policies


@pytest.fixture
def plugin_manager():
    pm = pluggy.PluginManager(policies.local_settings_hookspec.project_name)
    pm.add_hookspecs(policies)
    return pm


def test_local_settings_plain_function(plugin_manager: pluggy.PluginManager):
    """Test that a "plain" function from airflow_local_settings is registered via a plugin"""
    called = False

    def dag_policy(dag):
        nonlocal called
        called = True

    mod = Namespace(dag_policy=dag_policy)

    policies.make_plugin_from_local_settings(plugin_manager, mod, ["dag_policy"])

    plugin_manager.hook.dag_policy(dag="a")

    assert called


def test_local_settings_misnamed_argument(plugin_manager: pluggy.PluginManager):
    """
    If an function in local_settings doesn't have the "correct" name we can't naively turn it in to a
    plugin.

    This tests the sig-mismatch detection and shimming code path
    """
    called_with = None

    def dag_policy(wrong_arg_name):
        nonlocal called_with
        called_with = wrong_arg_name

    mod = Namespace(dag_policy=dag_policy)

    policies.make_plugin_from_local_settings(plugin_manager, mod, ["dag_policy"])

    plugin_manager.hook.dag_policy(dag="passed_dag_value")

    assert called_with == "passed_dag_value"
