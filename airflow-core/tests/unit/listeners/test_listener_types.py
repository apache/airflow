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

import pytest

from airflow.listeners import hookimpl
from airflow.listeners.listener import get_listener_manager
from airflow.listeners.types import FailureDetails


class TestFailureDetails:
    """AIP-97 foundation primitive: structured executor-side failure context."""

    @pytest.mark.parametrize(
        ("executor_kind", "infra_reason", "infra_metadata"),
        [
            pytest.param("kubernetes", "OOMKilled", {"exit_code": 137}, id="k8s-oom"),
            pytest.param(
                "kubernetes", "PodEvicted", {"node": "n-3", "evicted_by": "node-pressure"}, id="k8s-evicted"
            ),
            pytest.param("celery", "WorkerLost", {"worker_pid": 4321}, id="celery-worker-lost"),
            pytest.param("local_executor", "SIGKILL", {"signal": 9}, id="local-sigkill"),
            pytest.param("kubernetes", None, {}, id="unknown-reason"),
        ],
    )
    def test_construct(self, executor_kind, infra_reason, infra_metadata):
        details = FailureDetails(
            executor_kind=executor_kind,
            infra_reason=infra_reason,
            infra_metadata=infra_metadata,
        )
        assert details.executor_kind == executor_kind
        assert details.infra_reason == infra_reason
        assert details.infra_metadata == infra_metadata

    def test_default_metadata_is_empty_dict(self):
        details = FailureDetails(executor_kind="kubernetes", infra_reason="OOMKilled")
        assert details.infra_metadata == {}

    def test_frozen(self):
        """FailureDetails is immutable so listeners can safely cache it."""
        import attrs

        details = FailureDetails(executor_kind="kubernetes")
        with pytest.raises(attrs.exceptions.FrozenInstanceError):
            details.executor_kind = "celery"  # type: ignore[misc]


class TestOnTaskInstanceFailedAcceptsFailureDetails:
    """The on_task_instance_failed hookspec accepts the optional
    ``failure_details`` argument so listener authors can opt in to
    receiving infrastructure-side failure context as executors begin
    populating it."""

    def test_listener_with_failure_details_receives_it(self, listener_manager):
        received: dict[str, FailureDetails | None] = {"failure_details": None}

        # Per the hookspec docstring, listener implementations must declare
        # failure_details WITHOUT a default value — pluggy treats the impl
        # default as authoritative and silently overrides the caller's value.
        class InfraListener:
            @hookimpl
            def on_task_instance_failed(
                self,
                previous_state,
                task_instance,
                error,
                failure_details,
            ):
                received["failure_details"] = failure_details

        listener_manager(InfraListener())

        details = FailureDetails(
            executor_kind="kubernetes",
            infra_reason="OOMKilled",
            infra_metadata={"exit_code": 137},
        )
        get_listener_manager().hook.on_task_instance_failed(
            previous_state=None,
            task_instance=None,
            error=RuntimeError("boom"),
            failure_details=details,
        )

        assert received["failure_details"] == details

    def test_listener_without_failure_details_param_keeps_working(self, listener_manager):
        """Pluggy dispatches by parameter name, so existing hookimpls that
        don't declare ``failure_details`` continue to work unchanged."""
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
            failure_details=FailureDetails(executor_kind="kubernetes"),
        )

        assert called["count"] == 1
