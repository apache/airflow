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
"""Tests for the ListenerManager core functionality."""

from __future__ import annotations

from airflow_shared.listeners import hookimpl
from airflow_shared.listeners.listener import ListenerManager, get_listener_manager


class TestListenerManager:
    """Test the core ListenerManager functionality."""

    def test_get_listener_manager_singleton(self):
        """Test that get_listener_manager returns the same instance."""
        lm1 = get_listener_manager()
        lm2 = get_listener_manager()
        assert lm1 is lm2

    def test_listener_manager_is_singleton(self):
        """Test that ListenerManager is a singleton."""
        lm1 = ListenerManager()
        lm2 = ListenerManager()
        assert lm1 is lm2

    def test_add_listener(self):
        """Test adding a listener module."""
        lm = get_listener_manager()

        # Create a simple test listener module
        class TestListener:
            @hookimpl
            def on_task_instance_running(self, previous_state, task_instance):
                pass

        test_listener = TestListener()

        # Clear existing listeners first
        lm.clear()

        # Add the listener
        lm.add_listener(test_listener)

        # Verify it was added
        assert test_listener in lm.pm.get_plugins()

    def test_clear_listeners(self):
        """Test clearing all listeners."""
        lm = get_listener_manager()

        # Create a simple test listener module
        class TestListener:
            @hookimpl
            def on_task_instance_running(self, previous_state, task_instance):
                pass

        test_listener = TestListener()

        # Add a listener
        lm.add_listener(test_listener)

        # Clear all listeners
        lm.clear()

        # Verify no plugins remain (except built-in ones)
        plugins = lm.pm.get_plugins()
        assert test_listener not in plugins

    def test_has_listeners(self):
        """Test checking if listeners are registered."""
        lm = get_listener_manager()

        # Clear first
        lm.clear()

        # Should have no listeners initially (except built-in)
        # Note: has_listeners checks if there are any hook implementations
        # We can't easily test this without actual hook implementations

        # Create a listener with actual hook implementation
        class TestListener:
            @hookimpl
            def on_task_instance_running(self, previous_state, task_instance):
                pass

        test_listener = TestListener()
        lm.add_listener(test_listener)

        # Now it should have listeners
        assert lm.has_listeners
