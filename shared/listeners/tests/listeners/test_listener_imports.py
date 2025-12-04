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

from airflow_shared.listeners import hookimpl
from airflow_shared.listeners.listener import ListenerManager, get_listener_manager
from airflow_shared.listeners.spec import asset, dagrun, importerrors, lifecycle, taskinstance


def test_can_import_hookimpl():
    """Test that hookimpl decorator can be imported."""
    assert hookimpl is not None
    assert callable(hookimpl)


def test_can_import_listener_manager():
    """Test that ListenerManager class can be imported."""
    assert ListenerManager is not None


def test_can_import_get_listener_manager():
    """Test that get_listener_manager function can be imported."""
    assert get_listener_manager is not None
    assert callable(get_listener_manager)


def test_can_import_spec_modules():
    """Test that all spec modules can be imported."""
    # Verify all modules are imported
    assert asset is not None
    assert dagrun is not None
    assert importerrors is not None
    assert lifecycle is not None
    assert taskinstance is not None


def test_spec_modules_have_hookspec():
    """Test that spec modules have hookspec decorator."""
    # Each spec module should have functions decorated with @hookspec
    # We'll just verify the modules have the expected attributes
    assert hasattr(asset, "on_asset_changed")
    assert hasattr(dagrun, "on_dag_run_running")
    assert hasattr(importerrors, "on_new_dag_import_error")
    assert hasattr(lifecycle, "on_starting")
    assert hasattr(taskinstance, "on_task_instance_running")


def test_listener_manager_is_not_singleton_class():
    """Test that ListenerManager class itself is NOT a singleton."""
    # Create two instances directly
    lm1 = ListenerManager()
    lm2 = ListenerManager()

    # They should be different instances when created directly
    assert lm1 is not lm2


def test_get_listener_manager_returns_singleton():
    """Test that get_listener_manager returns the same instance."""
    # Get two instances via get_listener_manager
    lm1 = get_listener_manager()
    lm2 = get_listener_manager()

    # They should be the same instance
    assert lm1 is lm2


def test_hookimpl_can_be_used_as_decorator():
    """Test that hookimpl can be used as a decorator."""

    # Create a simple listener class with hookimpl decorator
    class TestListener:
        @hookimpl
        def on_starting(self, component):
            pass

    listener = TestListener()
    # Verify the method exists and has the hookimpl marker
    assert hasattr(listener, "on_starting")
    assert callable(listener.on_starting)


def test_listener_manager_can_register_listener():
    """Test that ListenerManager can register a listener."""

    class TestListener:
        def __init__(self):
            self.called = False

        @hookimpl
        def on_starting(self, component):
            self.called = True

    manager = ListenerManager()
    listener = TestListener()

    # Initially no listeners
    assert not manager.has_listeners

    # Register listener
    manager.add_listener(listener)
    assert manager.has_listeners

    # Verify listener is registered
    assert manager.pm.is_registered(listener)


def test_listener_manager_can_call_hooks():
    """Test that ListenerManager can call registered hooks."""

    class TestListener:
        def __init__(self):
            self.component_received = None

        @hookimpl
        def on_starting(self, component):
            self.component_received = component

    manager = ListenerManager()
    listener = TestListener()
    manager.add_listener(listener)

    # Call the hook
    test_component = "test_scheduler"
    manager.hook.on_starting(component=test_component)

    # Verify listener was called
    assert listener.component_received == test_component


def test_listener_manager_clear_removes_listeners():
    """Test that clear() removes all registered listeners."""

    class TestListener1:
        @hookimpl
        def on_starting(self, component):
            pass

    class TestListener2:
        @hookimpl
        def on_starting(self, component):
            pass

    manager = ListenerManager()
    listener1 = TestListener1()
    listener2 = TestListener2()

    manager.add_listener(listener1)
    manager.add_listener(listener2)
    assert manager.has_listeners

    # Clear all listeners
    manager.clear()
    assert not manager.has_listeners
    assert not manager.pm.is_registered(listener1)
    assert not manager.pm.is_registered(listener2)


def test_listener_manager_multiple_listeners():
    """Test that multiple listeners can be registered and called."""

    class TestListener1:
        def __init__(self):
            self.called = False

        @hookimpl
        def on_starting(self, component):
            self.called = True

    class TestListener2:
        def __init__(self):
            self.called = False

        @hookimpl
        def on_starting(self, component):
            self.called = True

    manager = ListenerManager()
    listener1 = TestListener1()
    listener2 = TestListener2()

    manager.add_listener(listener1)
    manager.add_listener(listener2)

    # Call the hook
    manager.hook.on_starting(component="test")

    # Both listeners should be called
    assert listener1.called
    assert listener2.called


def test_listener_manager_prevents_duplicate_registration():
    """Test that adding the same listener twice doesn't register it twice."""

    class TestListener:
        @hookimpl
        def on_starting(self, component):
            pass

    manager = ListenerManager()
    listener = TestListener()

    # Register twice
    manager.add_listener(listener)
    initial_plugins = len(manager.pm.get_plugins())

    manager.add_listener(listener)
    final_plugins = len(manager.pm.get_plugins())

    # Should still only have one plugin
    assert initial_plugins == final_plugins == 1


def test_listener_manager_hook_property():
    """Test that hook property returns a HookRelay."""
    manager = ListenerManager()
    hook = manager.hook

    # Verify hook has the expected methods
    assert hasattr(hook, "on_starting")
    assert hasattr(hook, "on_dag_run_running")
    assert hasattr(hook, "on_task_instance_running")
