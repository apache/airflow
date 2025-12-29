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


class TestListenerManager:
    def test_add_listener(self):
        """Test listener registration."""

        class TestListener:
            def __init__(self):
                self.called = False

            @hookimpl
            def on_starting(self, component):
                self.called = True

        lm = ListenerManager()
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
        listener = TestListener()
        lm.add_listener(listener)

        test_component = "test_component"
        lm.hook.on_starting(component=test_component)

        assert listener.component_received == test_component

    def test_multiple_listeners_receive_hooks(self):
        """Test multiple listeners all receive hook calls."""

        class TestListener:
            def __init__(self):
                self.calls = []

            @hookimpl
            def on_starting(self, component):
                self.calls.append(component)

        lm = ListenerManager()
        listener1 = TestListener()
        listener2 = TestListener()
        lm.add_listener(listener1)
        lm.add_listener(listener2)

        test_component = "test"
        lm.hook.on_starting(component=test_component)

        assert listener1.calls == [test_component]
        assert listener2.calls == [test_component]

    def test_hookimpl_marker(self):
        """Test hookimpl marker is available and works."""

        class TestListener:
            @hookimpl
            def on_starting(self, component):
                pass

        # Verify the hookimpl decorator was applied
        assert hasattr(TestListener.on_starting, "airflow_impl")
