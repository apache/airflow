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

    policies.make_plugin_from_local_settings(plugin_manager, mod, {"dag_policy"})

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

    policies.make_plugin_from_local_settings(plugin_manager, mod, {"dag_policy"})

    plugin_manager.hook.dag_policy(dag="passed_dag_value")

    assert called_with == "passed_dag_value"


def test_local_settings_subset_of_parameters(plugin_manager: pluggy.PluginManager):
    """
    A local_settings function may declare a subset of a hookspec's parameters.

    ``task_instance_mutation_hook`` accepts ``(task_instance, dag_run)``; a legacy hook that only declares
    ``task_instance`` is registered as-is (no shim) and pluggy passes it just the argument it declares, so
    single-parameter hooks keep working after the hookspec gained the optional ``dag_run`` parameter.
    """
    called_with = None

    def local_hook(task_instance):
        nonlocal called_with
        called_with = task_instance

    mod = Namespace(task_instance_mutation_hook=local_hook)

    policies.make_plugin_from_local_settings(plugin_manager, mod, {"task_instance_mutation_hook"})

    plugin_manager.hook.task_instance_mutation_hook(task_instance="ti", dag_run="dr")

    assert called_with == "ti"


def test_local_settings_receives_all_declared_parameters(plugin_manager: pluggy.PluginManager):
    """A hook declaring ``(task_instance, dag_run)`` with no defaults receives both arguments."""
    received = {}

    def task_instance_mutation_hook(task_instance, dag_run):
        received["task_instance"] = task_instance
        received["dag_run"] = dag_run

    mod = Namespace(task_instance_mutation_hook=task_instance_mutation_hook)

    policies.make_plugin_from_local_settings(plugin_manager, mod, {"task_instance_mutation_hook"})

    plugin_manager.hook.task_instance_mutation_hook(task_instance="ti", dag_run="dr")

    assert received == {"task_instance": "ti", "dag_run": "dr"}


def test_local_settings_defaulted_parameter_is_still_forwarded(plugin_manager: pluggy.PluginManager):
    """A hook that gives a hookspec parameter a default still receives the passed value.

    Pluggy forwards a hookimpl only the parameters it declares *without* a default, so it would silently
    drop ``dag_run`` from ``def hook(task_instance, dag_run=None)`` and leave the default in place. The
    shim detects the defaulted hookspec parameter and forwards the call positionally so the ergonomic
    ``dag_run=None`` signature receives the real value.
    """
    received = {}

    def task_instance_mutation_hook(task_instance, dag_run=None):
        received["task_instance"] = task_instance
        received["dag_run"] = dag_run

    mod = Namespace(task_instance_mutation_hook=task_instance_mutation_hook)

    policies.make_plugin_from_local_settings(plugin_manager, mod, {"task_instance_mutation_hook"})

    plugin_manager.hook.task_instance_mutation_hook(task_instance="ti", dag_run="dr")

    assert received == {"task_instance": "ti", "dag_run": "dr"}


def test_local_settings_unknown_argument_still_raises(plugin_manager: pluggy.PluginManager):
    """A local_settings function declaring a name the hookspec does not have is still rejected loudly."""

    def dag_policy(not_a_real_parameter, dag): ...

    mod = Namespace(dag_policy=dag_policy)

    # ``not_a_real_parameter`` is not a subset of the hookspec params, so it is shimmed and forwarded
    # positionally -- pluggy then rejects the extra positional argument rather than silently ignoring it.
    policies.make_plugin_from_local_settings(plugin_manager, mod, {"dag_policy"})
    with pytest.raises(TypeError):
        plugin_manager.hook.dag_policy(dag="passed_dag_value")
