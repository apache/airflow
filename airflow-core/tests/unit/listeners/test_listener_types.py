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
    def test_listener_with_failure_kind_receives_it(self, listener_manager):
        received: dict[str, str | None] = {"failure_kind": None, "reason": None}

        # An implementation-side default overrides the caller's value in pluggy.
        class InfraListener:
            @hookimpl
            def on_task_instance_failed(
                self,
                previous_state,
                task_instance,
                error,
                failure_kind,
                reason,
            ):
                received["failure_kind"] = failure_kind
                received["reason"] = reason

        listener_manager(InfraListener())

        get_listener_manager().hook.on_task_instance_failed(
            previous_state=None,
            task_instance=None,
            error=RuntimeError("boom"),
            failure_kind="infra",
            reason="Evicted",
        )

        assert received == {"failure_kind": "infra", "reason": "Evicted"}

    def test_listener_without_failure_kind_param_keeps_working(self, listener_manager):
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
            reason=None,
        )

        assert called["count"] == 1
