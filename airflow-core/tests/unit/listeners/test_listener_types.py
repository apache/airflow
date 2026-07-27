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

from airflow.listeners import hookimpl
from airflow.listeners.listener import get_listener_manager


class TestOnTaskInstanceFailedAcceptsFailureKind:
    """The on_task_instance_failed hookspec accepts the optional ``failure_kind``
    argument (AIP-97) so listener authors can opt in to the failure source
    (``infra`` / ``application`` / ``timeout`` / ``manual``) without parsing the error."""

    def test_listener_with_failure_kind_receives_it(self, listener_manager):
        received: dict[str, str | None] = {"failure_kind": None}

        # Per the hookspec docstring, listener implementations must declare
        # failure_kind WITHOUT a default value — pluggy treats the impl
        # default as authoritative and silently overrides the caller's value.
        class InfraListener:
            @hookimpl
            def on_task_instance_failed(
                self,
                previous_state,
                task_instance,
                error,
                failure_kind,
            ):
                received["failure_kind"] = failure_kind

        listener_manager(InfraListener())

        get_listener_manager().hook.on_task_instance_failed(
            previous_state=None,
            task_instance=None,
            error=RuntimeError("boom"),
            failure_kind="infra",
        )

        assert received["failure_kind"] == "infra"

    def test_listener_without_failure_kind_param_keeps_working(self, listener_manager):
        """Pluggy dispatches by parameter name, so existing hookimpls that
        don't declare ``failure_kind`` continue to work unchanged."""
        called = {"count": 0}

        class LegacyListener:
            @hookimpl
            def on_task_instance_failed(self, previous_state, task_instance, error):
                called["count"] += 1

        listener_manager(LegacyListener())

        get_listener_manager().hook.on_task_instance_failed(
            previous_state=None,
            task_instance=None,
            error=RuntimeError("boom"),
            failure_kind="infra",
        )

        assert called["count"] == 1
