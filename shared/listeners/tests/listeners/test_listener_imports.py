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
"""Basic import tests for listeners shared module."""

from __future__ import annotations



def test_can_import_hookimpl():
    """Test that hookimpl decorator can be imported."""
    from airflow_shared.listeners import hookimpl

    assert hookimpl is not None
    assert hasattr(hookimpl, "__call__")


def test_can_import_listener_manager():
    """Test that ListenerManager class can be imported."""
    from airflow_shared.listeners.listener import ListenerManager

    assert ListenerManager is not None


def test_can_import_get_listener_manager():
    """Test that get_listener_manager function can be imported."""
    from airflow_shared.listeners.listener import get_listener_manager

    assert get_listener_manager is not None
    assert hasattr(get_listener_manager, "__call__")


def test_can_import_spec_modules():
    """Test that all spec modules can be imported."""
    from airflow_shared.listeners.spec import asset, dagrun, importerrors, lifecycle, taskinstance

    # Verify all modules are imported
    assert asset is not None
    assert dagrun is not None
    assert importerrors is not None
    assert lifecycle is not None
    assert taskinstance is not None


def test_spec_modules_have_hookspec():
    """Test that spec modules have hookspec decorator."""
    from airflow_shared.listeners.spec import asset, dagrun, importerrors, lifecycle, taskinstance

    # Each spec module should have functions decorated with @hookspec
    # We'll just verify the modules have the expected attributes
    assert hasattr(asset, "on_asset_changed")
    assert hasattr(dagrun, "on_dag_run_running")
    assert hasattr(importerrors, "on_new_dag_import_error")
    assert hasattr(lifecycle, "before_starting")
    assert hasattr(taskinstance, "on_task_instance_running")


def test_listener_manager_is_not_singleton_class():
    """Test that ListenerManager class itself is NOT a singleton."""
    from airflow_shared.listeners.listener import ListenerManager

    # Create two instances directly
    lm1 = ListenerManager()
    lm2 = ListenerManager()

    # They should be different instances when created directly
    assert lm1 is not lm2


def test_get_listener_manager_returns_singleton():
    """Test that get_listener_manager returns the same instance."""
    from airflow_shared.listeners.listener import get_listener_manager

    # Get two instances via get_listener_manager
    lm1 = get_listener_manager()
    lm2 = get_listener_manager()

    # They should be the same instance
    assert lm1 is lm2
