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

from airflow_shared.listeners import hookimpl
from airflow_shared.listeners.listener import ListenerManager
from airflow_shared.listeners.spec import lifecycle, taskinstance


class TestListenerManager:
    def test_initial_state_has_no_listeners(self):
        """Test that a new ListenerManager has no listeners."""
        lm = ListenerManager()
        assert not lm.has_listeners
        assert len(lm.pm.get_plugins()) == 0

    def test_add_hookspecs_registers_hooks(self):
        """Test that add_hookspecs makes hooks available."""
        lm = ListenerManager()
        lm.add_hookspecs(lifecycle)

        # Verify lifecycle hooks are now available
        assert hasattr(lm.hook, "on_starting")
        assert hasattr(lm.hook, "before_stopping")

    def test_add_multiple_hookspecs(self):
        """Test that multiple hookspecs can be registered."""
        lm = ListenerManager()
        lm.add_hookspecs(lifecycle)
        lm.add_hookspecs(taskinstance)

        # Verify hooks from both specs are available
        assert hasattr(lm.hook, "on_starting")
        assert hasattr(lm.hook, "on_task_instance_running")

    def test_add_listener(self):
        """Test listener registration."""

        class TestListener:
            def __init__(self):
                self.called = False

            @hookimpl
            def on_starting(self, component):
                self.called = True

        lm = ListenerManager()
        lm.add_hookspecs(lifecycle)
        listener = TestListener()
        lm.add_listener(listener)

        assert lm.has_listeners
        assert lm.pm.is_registered(listener)

    def test_duplicate_listener_registration(self):
        """Test adding same listener twice doesn't duplicate."""

        class TestListener:
            @hookimpl
            def on_starting(self, component):
                pass

        lm = ListenerManager()
        lm.add_hookspecs(lifecycle)
        listener = TestListener()
        lm.add_listener(listener)
        lm.add_listener(listener)

        # Should only be registered once
        assert len(lm.pm.get_plugins()) == 1

    def test_clear_listeners(self):
        """Test clearing listeners removes all registered listeners."""

        class TestListener:
            @hookimpl
            def on_starting(self, component):
                pass

        lm = ListenerManager()
        lm.add_hookspecs(lifecycle)
        listener1 = TestListener()
        listener2 = TestListener()
        lm.add_listener(listener1)
        lm.add_listener(listener2)

        assert lm.has_listeners
        assert len(lm.pm.get_plugins()) == 2

        lm.clear()

        assert not lm.has_listeners
        assert len(lm.pm.get_plugins()) == 0

    def test_hook_calling(self):
        """Test hooks can be called and listeners receive them."""

        class TestListener:
            def __init__(self):
                self.component_received = None

            @hookimpl
            def on_starting(self, component):
                self.component_received = component

        lm = ListenerManager()
        lm.add_hookspecs(lifecycle)
        listener = TestListener()
        lm.add_listener(listener)

        test_component = "test_component"
        lm.hook.on_starting(component=test_component)

        assert listener.component_received == test_component

    def test_taskinstance_hooks(self):
        """Test taskinstance hook specs work correctly."""

        class TaskInstanceListener:
            def __init__(self):
                self.events = []

            @hookimpl
            def on_task_instance_running(self, previous_state, task_instance):
                self.events.append(("running", task_instance))

            @hookimpl
            def on_task_instance_success(self, previous_state, task_instance):
                self.events.append(("success", task_instance))

            @hookimpl
            def on_task_instance_failed(self, previous_state, task_instance, error):
                self.events.append(("failed", task_instance, error))

        lm = ListenerManager()
        lm.add_hookspecs(taskinstance)
        listener = TaskInstanceListener()
        lm.add_listener(listener)

        mock_ti = "mock_task_instance"
        lm.hook.on_task_instance_running(previous_state=None, task_instance=mock_ti)
        lm.hook.on_task_instance_success(previous_state=None, task_instance=mock_ti)
        lm.hook.on_task_instance_failed(previous_state=None, task_instance=mock_ti, error="test error")

        assert listener.events == [
            ("running", mock_ti),
            ("success", mock_ti),
            ("failed", mock_ti, "test error"),
        ]
