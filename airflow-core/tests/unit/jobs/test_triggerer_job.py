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

import asyncio
import contextlib
import datetime
import os
import random
import selectors
import threading
import time
import typing
import uuid
from socket import socket, socketpair
from unittest import mock
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import greenback
import httpx
import msgspec
import psutil
import pytest
import pytest_asyncio
from asgiref.sync import async_to_sync
from opentelemetry import trace as otel_trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from structlog.typing import FilteringBoundLogger

from airflow._shared.timezones import timezone
from airflow.executors import workloads
from airflow.executors.workloads.task import TaskInstanceDTO
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import (
    _USER_ACTION_CANCEL_MSG,
    ToTriggerRunner,
    ToTriggerSupervisor,
    TriggerCommsDecoder,
    TriggererJobRunner,
    TriggerLoggingFactory,
    TriggerRunner,
    TriggerRunnerSupervisor,
    _make_trigger_span,
    messages,
)
from airflow.models import Trigger
from airflow.providers.standard.triggers.temporal import DateTimeTrigger, TimeDeltaTrigger
from airflow.sdk.execution_time.comms import ToSupervisor, ToTask, _RequestFrame, _ResponseFrame
from airflow.sdk.serde import serialize as serde_serialize
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.triggers.testing import FailureTrigger, SuccessTrigger
from airflow.utils.state import State

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_connections,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_jobs,
    clear_db_runs,
    clear_db_triggers,
    clear_db_variables,
    clear_db_xcom,
)

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_database():
    """Fixture that cleans the database before and after every test."""
    clear_db_connections()
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_xcom()
    clear_db_variables()
    clear_db_triggers()
    clear_db_jobs()
    yield  # Test runs here
    clear_db_connections()
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_xcom()
    clear_db_variables()
    clear_db_triggers()
    clear_db_jobs()


def test_is_needed(session):
    """Checks the triggerer-is-needed logic"""
    # No triggers, no need
    triggerer_job = Job(heartrate=10, state=State.RUNNING)
    triggerer_job_runner = TriggererJobRunner(triggerer_job)
    assert triggerer_job_runner.is_needed() is False
    # Add a trigger, it's needed
    trigger = TimeDeltaTrigger(datetime.timedelta(days=7))
    trigger_orm = Trigger.from_object(trigger)
    session.add(trigger_orm)
    session.commit()
    assert triggerer_job_runner.is_needed() is True


def test_capacity_decode():
    """
    Tests that TriggererJob correctly sets capacity to a valid value passed in as a CLI arg,
    handles invalid args, or sets it to a default value if no arg is passed.
    """
    # Positive cases
    variants = [
        42,
        None,
    ]
    for input_str in variants:
        job = Job()
        job_runner = TriggererJobRunner(job, capacity=input_str)
        assert job_runner.capacity == input_str or job_runner.capacity == 1000

    # Negative cases
    variants = [
        "NAN",
        0.5,
        -42,
        4 / 2,  # Resolves to a float, in addition to being just plain weird
    ]
    for input_str in variants:
        job = Job()
        with pytest.raises(ValueError, match=r"Capacity number .+ is invalid"):
            TriggererJobRunner(job=job, capacity=input_str)


@pytest.mark.parametrize("team_name", ["team_a", None])
def test_triggerer_job_runner_stores_team_name(team_name):
    """TriggererJobRunner stores team_name as-is (validated at CLI layer)."""
    job = Job()
    runner = TriggererJobRunner(job, capacity=10, team_name=team_name)
    assert runner.team_name == team_name


def _make_remote_ti() -> TaskInstanceDTO:
    return TaskInstanceDTO(
        id=uuid.uuid4(),
        dag_version_id=uuid.uuid4(),
        task_id="task",
        dag_id="dag",
        run_id="run",
        try_number=1,
        pool_slots=1,
        queue="default",
        priority_weight=1,
    )


def _make_run_trigger(trigger_id: int, *, with_ti: bool = False) -> workloads.RunTrigger:
    return workloads.RunTrigger(
        id=trigger_id,
        classpath="airflow.triggers.testing.SuccessTrigger",
        encrypted_kwargs="x",
        ti=_make_remote_ti() if with_ti else None,
    )


def _make_http_status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "http://in-process.invalid./triggers/cleanup")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("boom", request=request, response=response)


@pytest.fixture
def supervisor_builder(mocker):
    """Build a TriggerRunnerSupervisor (``job=None``) with a mocked client and a registered id.

    AIP-92: the supervisor holds no metadata-DB access, so orchestration goes through a mocked
    Execution API client rather than a real DB session.
    """

    def builder():
        from airflow.sdk.api.client import Client

        process = mocker.Mock(spec=psutil.Process, pid=42)
        mock_stdin = mocker.Mock(spec=socket)
        mock_stdin.write = mocker.Mock()
        mock_stdin.sendall = mocker.Mock()

        proc = TriggerRunnerSupervisor(
            process_log=mocker.Mock(spec=FilteringBoundLogger),
            id=uuid.uuid4(),
            job=None,
            pid=process.pid,
            stdin=mock_stdin,
            process=process,
            capacity=10,
        )
        mock_selector = mocker.Mock(spec=selectors.DefaultSelector)
        mock_selector.select.return_value = []
        proc.selector = mock_selector

        # Mock the client so no HTTP/DB is touched, and pre-register the Job id.
        mock_client = mocker.Mock(spec=Client)
        mocker.patch.object(TriggerRunnerSupervisor, "make_client", return_value=mock_client)
        proc._triggerer_id = 7
        return proc

    return builder


def test_supervisor_stores_team_name(mocker):
    """TriggerRunnerSupervisor stores team_name field."""
    process = mocker.Mock(spec=psutil.Process, pid=99)
    mock_stdin = mocker.Mock(spec=socket)

    proc = TriggerRunnerSupervisor(
        process_log=mocker.Mock(spec=FilteringBoundLogger),
        id=uuid.uuid4(),
        job=None,
        pid=process.pid,
        stdin=mock_stdin,
        process=process,
        capacity=10,
        team_name="team_x",
    )
    assert proc.team_name == "team_x"

    proc_global = TriggerRunnerSupervisor(
        process_log=mocker.Mock(spec=FilteringBoundLogger),
        id=uuid.uuid4(),
        job=None,
        pid=process.pid,
        stdin=mock_stdin,
        process=process,
        capacity=10,
        team_name=None,
    )
    assert proc_global.team_name is None


def test_run_invokes_seams_in_order(supervisor_builder, mocker):
    """run() enters run_context, drives run_once while not should_stop, then exits run_context."""
    from contextlib import contextmanager

    supervisor = supervisor_builder()
    events: list[str] = []

    @contextmanager
    def fake_run_context(self):
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    counter = {"n": 0}

    def fake_run_once(self):
        counter["n"] += 1
        events.append(f"tick-{counter['n']}")

    mocker.patch.object(TriggerRunnerSupervisor, "run_context", fake_run_context)
    mocker.patch.object(TriggerRunnerSupervisor, "run_once", fake_run_once)
    mocker.patch.object(TriggerRunnerSupervisor, "should_stop", side_effect=lambda: counter["n"] >= 3)
    mocker.patch.object(TriggerRunnerSupervisor, "is_alive", return_value=True)

    supervisor.run()

    assert events == ["enter", "tick-1", "tick-2", "tick-3", "exit"]


def test_client_delegates_to_make_client_and_caches_result(mocker):
    """``supervisor.client`` delegates to ``make_client`` and caches the result across accesses."""
    make_client = mocker.patch.object(
        TriggerRunnerSupervisor,
        "make_client",
        autospec=True,
        return_value=mocker.sentinel.client,
    )
    process = mocker.Mock(spec=psutil.Process, pid=42)
    supervisor = TriggerRunnerSupervisor(
        process_log=mocker.Mock(spec=FilteringBoundLogger),
        id=uuid.uuid4(),
        job=None,
        pid=process.pid,
        stdin=mocker.Mock(spec=socket),
        process=process,
        capacity=10,
    )

    first = supervisor.client
    second = supervisor.client

    assert first is second is mocker.sentinel.client  # cached — same object
    make_client.assert_called_once_with(supervisor)


def test_run_context_exits_when_subprocess_dies(supervisor_builder, mocker):
    """Breaking out of the loop on a dead subprocess still unwinds run_context."""
    from contextlib import contextmanager

    supervisor = supervisor_builder()
    events: list[str] = []

    @contextmanager
    def fake_run_context(self):
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    mocker.patch.object(TriggerRunnerSupervisor, "run_context", fake_run_context)
    mocker.patch.object(TriggerRunnerSupervisor, "run_once", side_effect=lambda: events.append("tick"))
    mocker.patch.object(TriggerRunnerSupervisor, "should_stop", return_value=False)
    mocker.patch.object(TriggerRunnerSupervisor, "is_alive", side_effect=[True, False])

    supervisor.run()

    assert events == ["enter", "tick", "exit"]


@pytest.fixture
def jobless_supervisor(mocker):
    """Build a TriggerRunnerSupervisor (``job=None``) with a mocked client and a registered id."""
    from airflow.sdk.api.client import Client

    process = mocker.Mock(spec=psutil.Process, pid=42)
    mock_stdin = mocker.Mock(spec=socket)
    mock_stdin.write = mocker.Mock()
    mock_stdin.sendall = mocker.Mock()

    supervisor = TriggerRunnerSupervisor(
        process_log=mocker.Mock(spec=FilteringBoundLogger),
        id=uuid.uuid4(),
        job=None,
        pid=process.pid,
        stdin=mock_stdin,
        process=process,
        capacity=10,
    )
    mock_selector = mocker.Mock(spec=selectors.DefaultSelector)
    mock_selector.select.return_value = []
    supervisor.selector = mock_selector

    mock_client = mocker.Mock(spec=Client)
    mocker.patch.object(TriggerRunnerSupervisor, "make_client", return_value=mock_client)
    supervisor._triggerer_id = 7
    return supervisor


def test_start_without_job_generates_uuid_id(mocker):
    """start() called without a Job should generate a UUID for the supervisor id."""
    from airflow.sdk.execution_time.supervisor import WatchedSubprocess

    fake_proc = mocker.Mock()
    captured: dict = {}

    @classmethod
    def fake_super_start(cls, **kwargs):
        captured.update(kwargs)
        return fake_proc

    mocker.patch.object(WatchedSubprocess, "start", fake_super_start)

    TriggerRunnerSupervisor.start(capacity=10)

    assert isinstance(captured["id"], uuid.UUID)
    assert captured["job"] is None
    fake_proc.send_msg.assert_called_once()


class TestRunContextRegistersJob:
    def _build(self, mocker, register_returns=99):
        from airflow.sdk.api.client import Client

        process = mocker.Mock(spec=psutil.Process, pid=42)
        mock_stdin = mocker.Mock(spec=socket)
        sup = TriggerRunnerSupervisor(
            process_log=mocker.Mock(spec=FilteringBoundLogger),
            id=uuid.uuid4(),
            job=None,
            pid=process.pid,
            stdin=mock_stdin,
            process=process,
            capacity=10,
        )
        mock_client = mocker.Mock(spec=Client)
        mock_client.jobs.register.return_value = register_returns
        mocker.patch.object(TriggerRunnerSupervisor, "make_client", return_value=mock_client)
        return sup, mock_client

    def test_run_context_registers_job_and_sets_id(self, mocker):
        sup, mock_client = self._build(mocker)
        mocker.patch("airflow.jobs.triggerer_job_runner.get_hostname", return_value="triggerer-host")

        with sup.run_context():
            assert sup._triggerer_id == 99
        # Registers with the triggerer's own hostname (not the api-server's).
        mock_client.jobs.register.assert_called_once_with("TriggererJob", "triggerer-host")
        # Finalizes the Job on clean exit so it isn't left looking RUNNING forever.
        mock_client.jobs.complete.assert_called_once_with(99)

    def test_run_context_completes_job_even_on_error(self, mocker):
        sup, mock_client = self._build(mocker)
        mocker.patch("airflow.jobs.triggerer_job_runner.get_hostname", return_value="triggerer-host")

        with pytest.raises(RuntimeError, match="boom"):
            with sup.run_context():
                raise RuntimeError("boom")
        mock_client.jobs.complete.assert_called_once_with(99)

    def test_register_retries_until_api_server_ready(self, mocker):
        # The triggerer can boot before the api-server is ready; a transient error means "wait".
        sup, mock_client = self._build(mocker)
        mocker.patch("airflow.jobs.triggerer_job_runner.get_hostname", return_value="triggerer-host")
        mock_client.jobs.register.side_effect = [httpx.ReadTimeout("not ready yet"), 99]
        sleep = mocker.patch("airflow.jobs.triggerer_job_runner.time.sleep")

        assert sup._register_triggerer_job() == 99
        assert mock_client.jobs.register.call_count == 2
        sleep.assert_called_once()

    def test_register_reraises_non_transient_error(self, mocker):
        # A 4xx is a real error, not a startup race -- fail loudly without retrying.
        sup, mock_client = self._build(mocker)
        mocker.patch("airflow.jobs.triggerer_job_runner.get_hostname", return_value="triggerer-host")
        request = httpx.Request("POST", "http://api/execution/jobs")
        mock_client.jobs.register.side_effect = httpx.HTTPStatusError(
            "bad request", request=request, response=httpx.Response(400, request=request)
        )
        mocker.patch("airflow.jobs.triggerer_job_runner.time.sleep")

        with pytest.raises(httpx.HTTPStatusError):
            sup._register_triggerer_job()
        mock_client.jobs.register.assert_called_once()

    def test_trigger_log_id_is_registered_job_id(self, supervisor_builder):
        # The on-disk filename uses the int Job id so the reader (add_triggerer_suffix) finds it.
        assert supervisor_builder()._trigger_log_id() == 7


class TestOverridesDelegateToClient:
    def test_load_triggers_calls_client_and_reuses_update(self, supervisor_builder, mocker):
        supervisor = supervisor_builder()
        supervisor.client.triggers.load.return_value = [1, 2, 3]
        update = mocker.patch.object(TriggerRunnerSupervisor, "update_triggers")

        supervisor.load_triggers()

        supervisor.client.triggers.load.assert_called_once_with(
            triggerer_id=7,
            capacity=supervisor.capacity,
            health_check_threshold=supervisor.health_check_threshold,
            queues=None,
            team_name=None,
        )
        update.assert_called_once_with({1, 2, 3})

    def test_load_triggers_passes_team_name(self, supervisor_builder, mocker):
        """load_triggers passes team_name/queues to client.triggers.load."""
        supervisor = supervisor_builder()
        supervisor.team_name = "team_x"
        supervisor.queues = {"q1"}
        supervisor.client.triggers.load.return_value = [1, 2]
        mocker.patch.object(TriggerRunnerSupervisor, "update_triggers")

        supervisor.load_triggers()

        supervisor.client.triggers.load.assert_called_once_with(
            triggerer_id=7,
            capacity=supervisor.capacity,
            health_check_threshold=supervisor.health_check_threshold,
            queues=["q1"],
            team_name="team_x",
        )

    def test_load_triggers_swallows_transient_error(self, supervisor_builder, mocker):
        supervisor = supervisor_builder()
        supervisor.client.triggers.load.side_effect = httpx.ConnectError("boom")
        update = mocker.patch.object(TriggerRunnerSupervisor, "update_triggers")

        # Must not raise -- a blip should skip the cycle.
        supervisor.load_triggers()

        update.assert_not_called()

    def test_build_trigger_workloads_calls_client(self, supervisor_builder, mocker):
        supervisor = supervisor_builder()
        asset_workload = _make_run_trigger(11)
        task_workload = _make_run_trigger(12, with_ti=True)
        supervisor.client.triggers.workloads.return_value = [asset_workload, task_workload]
        register = mocker.patch.object(TriggerRunnerSupervisor, "_register_trigger_logger")
        mocker.patch.object(TriggerRunnerSupervisor, "_render_log_path", return_value="rendered.log")

        result = supervisor.build_trigger_workloads({12, 11})

        assert result == [asset_workload, task_workload]
        (called_arg,), _ = supervisor.client.triggers.workloads.call_args
        assert sorted(called_arg) == [11, 12]
        # Only the task-backed workload gets a logger registered (asset-based has no ti).
        register.assert_called_once_with(12, task_workload.ti, "rendered.log")

    def test_build_trigger_workloads_returns_empty_on_transient_error(self, supervisor_builder, mocker):
        supervisor = supervisor_builder()
        supervisor.client.triggers.workloads.side_effect = httpx.ConnectError("boom")
        register = mocker.patch.object(TriggerRunnerSupervisor, "_register_trigger_logger")

        # Returns [] so the triggers stay assigned and get re-fetched next load cycle.
        assert supervisor.build_trigger_workloads({1}) == []
        register.assert_not_called()

    def test_on_trigger_event_calls_submit_event(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor.on_trigger_event(5, TriggerEvent(payload={"result": "ok"}))
        # The payload is serde-serialized so non-JSON-native values survive the hop; the server
        # splices it into next_kwargs without deserializing (the worker deserializes on resume).
        supervisor.client.triggers.submit_event.assert_called_once_with(5, serde_serialize({"result": "ok"}))

    def test_on_trigger_failure_calls_submit_failure(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor.on_trigger_failure(5, ["trace", "back"])
        supervisor.client.triggers.submit_failure.assert_called_once_with(5, ["trace", "back"])

    def test_clean_unused_calls_cleanup(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor.clean_unused()
        supervisor.client.triggers.cleanup.assert_called_once_with()

    def test_clean_unused_swallows_transient_error(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor.client.triggers.cleanup.side_effect = httpx.ConnectError("boom")
        # Must not raise.
        supervisor.clean_unused()

    def test_clean_unused_reraises_non_transient_4xx(self, supervisor_builder):
        # A persistent 4xx is a logic bug the client never retries; it must crash loudly
        # instead of looping forever at WARNING.
        supervisor = supervisor_builder()
        supervisor.client.triggers.cleanup.side_effect = _make_http_status_error(400)
        with pytest.raises(httpx.HTTPStatusError):
            supervisor.clean_unused()

    def test_clean_unused_swallows_5xx(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor.client.triggers.cleanup.side_effect = _make_http_status_error(503)
        # 5xx is transient -- swallow it.
        supervisor.clean_unused()


class TestHandleEventsReQueue:
    def test_handle_events_submits_and_clears(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor.events.append((5, TriggerEvent(payload={"result": "ok"})))

        supervisor.handle_events()

        supervisor.client.triggers.submit_event.assert_called_once()
        assert not supervisor.events

    def test_handle_events_requeues_on_transient_error(self, supervisor_builder):
        supervisor = supervisor_builder()
        event = TriggerEvent(payload={"result": "ok"})
        supervisor.events.append((5, event))
        supervisor.client.triggers.submit_event.side_effect = httpx.ConnectError("boom")

        # Must not raise; the popped event is re-queued (not lost) for the next cycle.
        supervisor.handle_events()

        assert list(supervisor.events) == [(5, event)]

    def test_handle_events_reraises_non_transient(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor.events.append((5, TriggerEvent(payload={"result": "ok"})))
        supervisor.client.triggers.submit_event.side_effect = _make_http_status_error(400)

        with pytest.raises(httpx.HTTPStatusError):
            supervisor.handle_events()

    def test_handle_failed_triggers_submits_and_clears(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor.failed_triggers.append((5, ["trace", "back"]))

        supervisor.handle_failed_triggers()

        supervisor.client.triggers.submit_failure.assert_called_once_with(5, ["trace", "back"])
        assert not supervisor.failed_triggers

    def test_handle_failed_triggers_requeues_on_transient_error(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor.failed_triggers.append((5, ["trace"]))
        supervisor.client.triggers.submit_failure.side_effect = httpx.ConnectError("boom")

        supervisor.handle_failed_triggers()

        assert list(supervisor.failed_triggers) == [(5, ["trace"])]


class TestHeartbeat:
    def test_heartbeat_calls_jobs_heartbeat(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor._last_runner_comms = time.monotonic()
        supervisor._heartrate = 0  # disable throttle so it always fires
        supervisor.heartbeat()
        supervisor.client.jobs.heartbeat.assert_called_once_with(7)

    def test_heartbeat_throttled_to_heartrate(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor._last_runner_comms = time.monotonic()
        supervisor._heartrate = 100
        supervisor._last_heartbeat = time.monotonic()  # just beat -- within heartrate

        supervisor.heartbeat()

        supervisor.client.jobs.heartbeat.assert_not_called()

    def test_heartbeat_swallows_transient_error(self, supervisor_builder):
        supervisor = supervisor_builder()
        supervisor._last_runner_comms = time.monotonic()
        supervisor._heartrate = 0
        supervisor.client.jobs.heartbeat.side_effect = httpx.ConnectError("boom")

        # Must not raise -- a missed heartbeat retries next loop.
        supervisor.heartbeat()

    def test_heartbeat_watchdog(self, supervisor_builder):
        """heartbeat() fires when the subprocess is active, skips when silent, and only arms the
        silence flag once (so the error is logged once, not on every subsequent call)."""
        supervisor = supervisor_builder()
        supervisor._heartrate = 0  # force a beat each call; isolate the watchdog logic

        # Within threshold — heartbeat fires
        supervisor._last_runner_comms = time.monotonic() - 5.0
        supervisor.heartbeat()
        assert supervisor.client.jobs.heartbeat.call_count == 1

        # Just inside threshold (29.9s < 30s default) — still fires
        supervisor._last_runner_comms = time.monotonic() - 29.9
        supervisor.heartbeat()
        assert supervisor.client.jobs.heartbeat.call_count == 2

        # Beyond threshold — heartbeat skips and silence flag is set
        supervisor._last_runner_comms = time.monotonic() - 9999.0
        supervisor.heartbeat()
        assert supervisor.client.jobs.heartbeat.call_count == 2
        assert supervisor._runner_comms_silence_logged is True

        # Subsequent silent heartbeats don't re-arm the flag (error logged only once)
        supervisor.heartbeat()
        supervisor.heartbeat()
        assert supervisor.client.jobs.heartbeat.call_count == 2
        assert supervisor._runner_comms_silence_logged is True

        # Once the subprocess speaks again the flag resets and heartbeat resumes
        supervisor._last_runner_comms = time.monotonic()
        supervisor.heartbeat()
        assert supervisor.client.jobs.heartbeat.call_count == 3
        assert supervisor._runner_comms_silence_logged is False

    def test_heartbeat_watchdog_disabled_when_threshold_is_zero(self, supervisor_builder, mocker):
        """Setting runner_health_check_threshold=0 disables the watchdog; heartbeat always fires."""
        supervisor = supervisor_builder()
        supervisor._heartrate = 0
        mocker.patch.object(type(supervisor), "runner_health_check_threshold", new=0)

        supervisor._last_runner_comms = time.monotonic() - 9999.0

        supervisor.heartbeat()

        supervisor.client.jobs.heartbeat.assert_called_once()


class TestMetricTags:
    def test_metric_tags_uses_hostname(self, supervisor_builder, mocker):
        """metric_tags() returns {"hostname": get_hostname()} -- the supervisor has no Job row."""
        supervisor = supervisor_builder()
        mocker.patch("airflow.jobs.triggerer_job_runner.get_hostname", return_value="myhost")
        assert supervisor.metric_tags() == {"hostname": "myhost"}


def test_emit_metrics_uses_metric_tags_override(jobless_supervisor, mocker):
    """emit_metrics() applies the tags supplied by metric_tags() to every gauge."""
    gauge = mocker.patch("airflow.jobs.triggerer_job_runner.stats.gauge")
    mocker.patch.object(
        TriggerRunnerSupervisor,
        "metric_tags",
        return_value={"hostname": "astro-host", "deployment": "demo"},
    )

    jobless_supervisor.emit_metrics()

    assert gauge.call_count == 2
    for call in gauge.call_args_list:
        assert call.kwargs["tags"] == {"hostname": "astro-host", "deployment": "demo"}


def test_trigger_logger_close():
    logger = TriggerLoggingFactory(log_path="/tmp/test.log", ti=MagicMock())

    mock_fh = MagicMock()
    mock_fh.closed = False

    logger._filehandle = mock_fh

    logger.close()

    mock_fh.close.assert_called_once()


def test_trigger_logger_fd_closed_when_upload_to_remote_raises(jobless_supervisor):
    """If upload_to_remote() raises during finished-trigger cleanup, the FD must still be closed.

    Regression test for the file handle leak referenced in
    https://github.com/apache/airflow/discussions/65985 — without try/finally, a failed
    remote-log upload would skip ``factory.close()`` and leak the underlying BufferedWriter
    for every failed upload.
    """
    factory = MagicMock(spec=TriggerLoggingFactory)
    factory.upload_to_remote.side_effect = RuntimeError("simulated remote-logging failure")

    jobless_supervisor.logger_cache[42] = factory
    jobless_supervisor.running_triggers.add(42)

    msg = messages.TriggerStateChanges(finished=[42])
    jobless_supervisor._handle_request(msg, log=MagicMock(spec=FilteringBoundLogger), req_id=0)

    factory.upload_to_remote.assert_called_once()
    factory.close.assert_called_once()
    assert 42 not in jobless_supervisor.logger_cache
    assert 42 not in jobless_supervisor.running_triggers


class TestTriggerRunner:
    def test_blocked_main_thread_warning_threshold_decode(self) -> None:
        with conf_vars({("triggerer", "blocked_main_thread_warning_threshold"): "0.5"}):
            trigger_runner = TriggerRunner()

        assert trigger_runner.blocked_main_thread_warning_threshold == 0.5

    @pytest.mark.asyncio
    async def test_block_watchdog_does_not_log_when_threshold_is_not_exceeded(self) -> None:
        with conf_vars({("triggerer", "blocked_main_thread_warning_threshold"): "0.5"}):
            trigger_runner = TriggerRunner()

        trigger_runner.log = AsyncMock()

        async def fake_sleep(_):
            trigger_runner.stop = True

        with (
            patch("airflow.jobs.triggerer_job_runner.asyncio.sleep", side_effect=fake_sleep),
            patch("airflow.jobs.triggerer_job_runner.time.monotonic", side_effect=[1.0, 1.4]),
            patch("airflow.jobs.triggerer_job_runner.stats.incr") as mock_stats_incr,
        ):
            await trigger_runner.block_watchdog()

        trigger_runner.log.ainfo.assert_not_called()
        mock_stats_incr.assert_not_called()

    @pytest.mark.asyncio
    async def test_block_watchdog_logs_when_threshold_is_exceeded(self) -> None:
        with conf_vars({("triggerer", "blocked_main_thread_warning_threshold"): "0.5"}):
            trigger_runner = TriggerRunner()

        trigger_runner.log = AsyncMock()

        async def fake_sleep(_):
            trigger_runner.stop = True

        with (
            patch("airflow.jobs.triggerer_job_runner.asyncio.sleep", side_effect=fake_sleep),
            patch("airflow.jobs.triggerer_job_runner.time.monotonic", side_effect=[1.0, 1.6]),
            patch("airflow.jobs.triggerer_job_runner.stats.incr") as mock_stats_incr,
        ):
            await trigger_runner.block_watchdog()

        trigger_runner.log.ainfo.assert_awaited_once()
        log_message, elapsed, threshold = trigger_runner.log.ainfo.await_args.args
        assert "configured warning threshold" in log_message
        assert elapsed == pytest.approx(0.6)
        assert threshold == 0.5
        mock_stats_incr.assert_called_once_with("triggers.blocked_main_thread")

    def test_run_inline_trigger_canceled(self, session) -> None:
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {
            1: {"task": MagicMock(spec=asyncio.Task), "is_watcher": False, "name": "mock_name", "events": 0}
        }
        mock_trigger = MagicMock(spec=BaseTrigger)
        mock_trigger.timeout_after = None
        mock_trigger.run.side_effect = asyncio.CancelledError()
        mock_trigger.task_instance = MagicMock()
        mock_trigger.task_instance.map_index = -1

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(trigger_runner.run_trigger(1, mock_trigger))

    def test_run_inline_trigger_timeout(self, session, cap_structlog) -> None:
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {
            1: {"task": MagicMock(spec=asyncio.Task), "is_watcher": False, "name": "mock_name", "events": 0}
        }
        mock_trigger = MagicMock(spec=BaseTrigger)
        mock_trigger.run.side_effect = asyncio.CancelledError()
        mock_trigger.task_instance = MagicMock()
        mock_trigger.task_instance.map_index = -1

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(
                trigger_runner.run_trigger(
                    1, mock_trigger, timeout_after=timezone.utcnow() - datetime.timedelta(hours=1)
                )
            )
        assert {"event": "Trigger cancelled due to timeout", "log_level": "error"} in cap_structlog

    def test_run_trigger_calls_on_kill_for_user_action(self, session, cap_structlog) -> None:
        """on_kill() is called when CancelledError carries the user-action sentinel."""
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {
            1: {"task": MagicMock(spec=asyncio.Task), "is_watcher": False, "name": "mock_name", "events": 0}
        }
        mock_trigger = MagicMock(spec=BaseTrigger)
        mock_trigger.run.side_effect = asyncio.CancelledError(_USER_ACTION_CANCEL_MSG)
        mock_trigger.task_instance = MagicMock()
        mock_trigger.task_instance.map_index = -1
        mock_trigger.on_kill = AsyncMock()

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(trigger_runner.run_trigger(1, mock_trigger))

        mock_trigger.on_kill.assert_awaited_once()
        assert {
            "event": "Trigger cancelled by user action, invoking on_kill",
            "log_level": "info",
            "name": "mock_name",
        } in cap_structlog

    def test_run_trigger_skips_on_kill_without_user_action_message(self, session) -> None:
        """on_kill() is not called when CancelledError has no user-action sentinel (shutdown/EOF)."""
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {
            1: {"task": MagicMock(spec=asyncio.Task), "is_watcher": False, "name": "mock_name", "events": 0}
        }
        mock_trigger = MagicMock(spec=BaseTrigger)
        mock_trigger.run.side_effect = asyncio.CancelledError()
        mock_trigger.task_instance = MagicMock()
        mock_trigger.task_instance.map_index = -1
        mock_trigger.on_kill = AsyncMock()

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(trigger_runner.run_trigger(1, mock_trigger))

        mock_trigger.on_kill.assert_not_called()

    def test_run_trigger_on_kill_exception_does_not_swallow_cancelled_error(self, session) -> None:
        """CancelledError propagates even if on_kill() raises."""
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {
            1: {"task": MagicMock(spec=asyncio.Task), "is_watcher": False, "name": "mock_name", "events": 0}
        }
        mock_trigger = MagicMock(spec=BaseTrigger)
        mock_trigger.run.side_effect = asyncio.CancelledError(_USER_ACTION_CANCEL_MSG)
        mock_trigger.task_instance = MagicMock()
        mock_trigger.task_instance.map_index = -1
        mock_trigger.on_kill = AsyncMock(side_effect=RuntimeError("external API down"))
        mock_trigger.cleanup = AsyncMock()

        with pytest.raises(asyncio.CancelledError):
            asyncio.run(trigger_runner.run_trigger(1, mock_trigger))

        mock_trigger.on_kill.assert_awaited_once()
        mock_trigger.cleanup.assert_awaited_once()

    def test_run_trigger_routes_shared_stream_trigger_through_manager(self, session) -> None:
        """A BaseEventTrigger that opts into a shared stream consumes filter_shared_stream()."""
        from airflow.triggers.base import BaseEventTrigger, TriggerEvent

        class _SharedTrigger(BaseEventTrigger):
            def __init__(self, queue_url: str, region: str | None = None):
                super().__init__()
                self.queue_url = queue_url
                self.region = region

            def serialize(self):
                return (
                    f"{type(self).__module__}.{type(self).__qualname__}",
                    {"queue_url": self.queue_url, "region": self.region},
                )

            def shared_stream_key(self):
                return ("queue", self.queue_url)

            @classmethod
            async def open_shared_stream(cls, kwargs):
                yield {"region": "us"}
                yield {"region": "eu"}
                # Stay alive so the manager can tear us down on unsubscribe.
                await asyncio.Event().wait()

            async def filter_shared_stream(self, shared_stream):
                async for raw in shared_stream:
                    if self.region is None or raw["region"] == self.region:
                        yield TriggerEvent(raw)

            async def run(self):  # pragma: no cover - replaced by filter_shared_stream
                yield TriggerEvent({})

        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {
            1: {"task": MagicMock(spec=asyncio.Task), "is_watcher": True, "name": "us", "events": 0}
        }
        trigger = _SharedTrigger(queue_url="https://q", region="us")
        trigger.task_instance = MagicMock()
        trigger.task_instance.map_index = -1

        async def _drive():
            run_task = asyncio.create_task(trigger_runner.run_trigger(1, trigger))
            # Wait until the "us" event has been pushed onto the outbound queue,
            # then cancel the trigger so the test can exit deterministically.
            for _ in range(100):
                await asyncio.sleep(0.01)
                if trigger_runner.events:
                    break
            run_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await run_task

        asyncio.run(_drive())

        events = list(trigger_runner.events)
        assert len(events) == 1
        trigger_id, event = events[0]
        assert trigger_id == 1
        assert event.payload == {"region": "us"}
        # Group is torn down on unsubscribe.
        assert trigger_runner._shared_streams._groups == {}

    def test_run_trigger_on_kill_timeout_does_not_block_cleanup(self, session) -> None:
        """A hanging on_kill() is interrupted after the timeout and cleanup still runs."""
        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {
            1: {"task": MagicMock(spec=asyncio.Task), "is_watcher": False, "name": "mock_name", "events": 0}
        }
        mock_trigger = MagicMock(spec=BaseTrigger)
        mock_trigger.run.side_effect = asyncio.CancelledError(_USER_ACTION_CANCEL_MSG)
        mock_trigger.task_instance = MagicMock()
        mock_trigger.task_instance.map_index = -1

        async def hanging_on_kill():
            await asyncio.sleep(9999)

        mock_trigger.on_kill = hanging_on_kill
        mock_trigger.cleanup = AsyncMock()

        with patch("airflow.jobs.triggerer_job_runner._ON_CANCEL_TIMEOUT", 0.01):
            with pytest.raises(asyncio.CancelledError):
                asyncio.run(trigger_runner.run_trigger(1, mock_trigger))

        mock_trigger.cleanup.assert_awaited_once()

    @patch("airflow.jobs.triggerer_job_runner.Trigger._decrypt_kwargs")
    @patch(
        "airflow.jobs.triggerer_job_runner.TriggerRunner.get_trigger_by_classpath",
        return_value=DateTimeTrigger,
    )
    @pytest.mark.asyncio
    async def test_update_trigger_with_triggerer_argument_change(
        self, mock_get_trigger_by_classpath, mock_decrypt_kwargs, session, cap_structlog
    ) -> None:
        trigger_runner = TriggerRunner()

        def fn(moment): ...

        mock_decrypt_kwargs.return_value = {"moment": ..., "not_exists_arg": ...}
        mock_get_trigger_by_classpath.return_value = fn

        trigger_runner.to_create.append(
            workloads.RunTrigger.model_construct(id=1, classpath="abc", encrypted_kwargs="fake"),
        )
        await trigger_runner.create_triggers()

        assert "Trigger failed" in cap_structlog.text
        err = cap_structlog[0]["error"]
        assert isinstance(err, TypeError)
        assert "got an unexpected keyword argument 'not_exists_arg'" in str(err)

    @pytest.mark.asyncio
    @patch("airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True)
    async def test_invalid_trigger(self, supervisor_builder):
        """Test the behaviour when we try to run an invalid Trigger"""
        workload = workloads.RunTrigger.model_construct(
            id=1, ti=None, classpath="fake.classpath", encrypted_kwargs={}
        )
        trigger_runner = TriggerRunner()
        trigger_runner.comms_decoder = AsyncMock(spec=TriggerCommsDecoder)
        trigger_runner.comms_decoder.asend.return_value = messages.TriggerStateSync(
            to_create=[], to_cancel=set()
        )

        trigger_runner.to_create.append(workload)

        await trigger_runner.create_triggers()
        assert (1, ANY) in trigger_runner.failed_triggers
        ids = await trigger_runner.cleanup_finished_triggers()
        await trigger_runner.sync_state_to_supervisor(ids)

        # Check that we sent the right info in the failure message
        assert trigger_runner.comms_decoder.asend.call_count == 1
        msg = trigger_runner.comms_decoder.asend.mock_calls[0].args[0]
        assert isinstance(msg, messages.TriggerStateChanges)

        assert msg.events is None
        assert msg.failures is not None
        assert len(msg.failures) == 1
        trigger_id, traceback = msg.failures[0]
        assert trigger_id == 1
        assert traceback[-1] == "ModuleNotFoundError: No module named 'fake'\n"

    @pytest.mark.asyncio
    async def test_trigger_kwargs_serialization_cleanup(self, session):
        """
        Test that trigger kwargs are properly cleaned of serialization artifacts
        (__var, __type keys).
        """
        from airflow.serialization.serialized_objects import BaseSerialization

        kw = {"simple": "test", "tuple": (), "dict": {}, "list": []}

        serialized_kwargs = BaseSerialization.serialize(kw)

        trigger_orm = Trigger(classpath="airflow.triggers.testing.SuccessTrigger", kwargs=serialized_kwargs)
        session.add(trigger_orm)
        session.commit()

        stored_kwargs = trigger_orm.kwargs
        assert stored_kwargs == kw

        runner = TriggerRunner()
        runner.to_create.append(
            workloads.RunTrigger.model_construct(
                id=trigger_orm.id,
                ti=None,
                classpath=trigger_orm.classpath,
                encrypted_kwargs=trigger_orm.encrypted_kwargs,
            )
        )

        await runner.create_triggers()
        assert trigger_orm.id in runner.triggers
        trigger_instance = runner.triggers[trigger_orm.id]["task"]

        # The test passes if no exceptions were raised during trigger creation
        trigger_instance.cancel()
        await runner.cleanup_finished_triggers()

    @pytest.mark.asyncio
    @patch("airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True)
    async def test_sync_state_to_supervisor(self, supervisor_builder):
        trigger_runner = TriggerRunner()
        trigger_runner.comms_decoder = AsyncMock(spec=TriggerCommsDecoder)
        trigger_runner.events.append((1, TriggerEvent(payload={"status": "SUCCESS"})))
        trigger_runner.events.append((2, TriggerEvent(payload={"status": "FAILED"})))
        trigger_runner.events.append((3, TriggerEvent(payload={"status": "SUCCESS", "data": object()})))

        async def asend_side_effect(msg):
            if msg.events and len(msg.events) == 3:
                raise NotImplementedError("Simulate non-serializable event")
            return messages.TriggerStateSync(to_create=[], to_cancel=set())

        trigger_runner.comms_decoder.asend.side_effect = asend_side_effect

        await trigger_runner.sync_state_to_supervisor(finished_ids=[])

        assert trigger_runner.comms_decoder.asend.call_count == 2

        first_call = trigger_runner.comms_decoder.asend.call_args_list[0].args[0]
        second_call = trigger_runner.comms_decoder.asend.call_args_list[1].args[0]

        assert len(first_call.events) == 3
        assert len(second_call.events) == 2


@pytest.mark.execution_timeout(5)
def test_trigger_runner_exception_stops_triggerer(mocker):
    """
    Checks that if an exception occurs when creating triggers, that the triggerer
    process stops
    """
    import signal

    # The triggerer orchestrates through the Execution API; give its supervisor a mock client so
    # this test exercises the runner-died -> stop behaviour without a real api-server.
    mock_client = mocker.Mock()
    mock_client.jobs.register.return_value = 1
    mock_client.triggers.load.return_value = []
    mocker.patch.object(TriggerRunnerSupervisor, "make_client", return_value=mock_client)

    job_runner = TriggererJobRunner(Job())

    # Wait 4 seconds for the triggerer to stop
    try:

        def on_timeout(signum, frame):
            # _execute() sets up trigger_runner asynchronously; on a slow runner the
            # timer can fire before the subprocess exists. Re-arm and try again rather
            # than dereferencing a not-yet-started runner.
            runner = job_runner.trigger_runner
            if runner is None:
                signal.setitimer(signal.ITIMER_REAL, 0.1)
                return
            os.kill(runner.pid, signal.SIGKILL)

        signal.signal(signal.SIGALRM, on_timeout)
        signal.setitimer(signal.ITIMER_REAL, 0.1)
        # This either returns cleanly, or the pytest timeout hits.
        assert job_runner._execute() == -9
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


@pytest.mark.asyncio
async def test_trigger_firing():
    """
    Checks that when a trigger fires, it correctly makes it into the
    event queue.
    """
    runner = TriggerRunner()

    runner.to_create.append(
        # Use a trigger that will immediately succeed
        workloads.RunTrigger.model_construct(
            id=1,
            ti=None,
            classpath=f"{SuccessTrigger.__module__}.{SuccessTrigger.__name__}",
            encrypted_kwargs='{"__type":"dict", "__var":{}}',
        ),
    )
    await runner.create_triggers()
    try:
        # Wait for up to 3 seconds for it to fire and appear in the event queue
        for _ in range(30):
            await asyncio.sleep(0.1)
            finished = await runner.cleanup_finished_triggers()
            if runner.events:
                assert list(runner.events) == [(1, TriggerEvent(True))]
                assert finished == [1]
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("TriggerRunner never sent the trigger event out")
    finally:
        for info in runner.triggers.values():
            info["task"].cancel()


@pytest.mark.asyncio
async def test_trigger_failing():
    """
    Checks that when a trigger fails, it correctly makes it into the
    failure queue.
    """
    runner = TriggerRunner()

    runner.to_create.append(
        # Use a trigger that will immediately fail
        workloads.RunTrigger.model_construct(
            id=1,
            ti=None,
            classpath=f"{FailureTrigger.__module__}.{FailureTrigger.__name__}",
            encrypted_kwargs='{"__type":"dict", "__var":{}}',
        ),
    )
    await runner.create_triggers()
    try:
        # Wait for up to 3 seconds for it to fire and appear in the event queue
        for _ in range(30):
            await asyncio.sleep(0.1)
            await runner.cleanup_finished_triggers()
            if runner.failed_triggers:
                assert len(runner.failed_triggers) == 1
                trigger_id, exc = runner.failed_triggers[0]
                assert trigger_id == 1
                assert isinstance(exc, ValueError)
                assert exc.args[0] == "Deliberate trigger failure"
                break
        else:
            pytest.fail("TriggerRunner never marked the trigger as failed")
    finally:
        for info in runner.triggers.values():
            info["task"].cancel()


def test_update_triggers_prevents_duplicate_creation_queue_entries(supervisor_builder):
    """
    update_triggers does not re-queue a trigger that is already queued for creation.

    The build step (``client.triggers.workloads``) is only consulted for ids not already known.
    """
    supervisor = supervisor_builder()
    supervisor.client.triggers.workloads.return_value = [_make_run_trigger(99)]

    # First call queues the trigger for creation.
    supervisor.update_triggers({99})
    assert len(supervisor.creating_triggers) == 1
    assert supervisor.creating_triggers[0].id == 99
    supervisor.client.triggers.workloads.assert_called_once_with([99])

    # Second call with the same id does not add it again (and does not re-fetch it).
    supervisor.update_triggers({99})
    assert len(supervisor.creating_triggers) == 1
    assert supervisor.creating_triggers[0].id == 99
    supervisor.client.triggers.workloads.assert_called_once_with([99])

    # The trigger is queued but not yet running or in any other tracking set.
    assert 99 not in supervisor.running_triggers
    assert 99 not in supervisor.cancelling_triggers
    assert not any(tid == 99 for tid, _ in supervisor.events)
    assert not any(tid == 99 for tid, _ in supervisor.failed_triggers)


def test_update_triggers_delegates_workload_creation(supervisor_builder, mocker):
    supervisor = supervisor_builder()
    supervisor.running_triggers = {1, 3}
    workload = workloads.RunTrigger(id=2, classpath="some.trigger", encrypted_kwargs="", ti=None)
    build_trigger_workloads = mocker.patch.object(
        TriggerRunnerSupervisor, "build_trigger_workloads", autospec=True, return_value=[workload]
    )

    supervisor.update_triggers({2, 3})

    build_trigger_workloads.assert_called_once_with(supervisor, {2})
    assert list(supervisor.creating_triggers) == [workload]
    assert supervisor.cancelling_triggers == {1}


def test_update_triggers_prevents_duplicate_creation_queue_entries_with_multiple_triggers(
    supervisor_builder,
):
    """update_triggers does not re-queue triggers already queued for creation, across multiple ids."""
    supervisor = supervisor_builder()
    supervisor.client.triggers.workloads.side_effect = lambda ids: [
        _make_run_trigger(tid) for tid in sorted(ids)
    ]

    # First call queues both triggers.
    supervisor.update_triggers({1, 2})
    assert {t.id for t in supervisor.creating_triggers} == {1, 2}

    # Second call with the same ids does not add them again.
    supervisor.update_triggers({1, 2})
    assert {t.id for t in supervisor.creating_triggers} == {1, 2}

    # Third call with just one already-queued id does not add duplicates.
    supervisor.update_triggers({1})
    assert {t.id for t in supervisor.creating_triggers} == {1, 2}


class TestTriggererJobRunner:
    @patch("airflow.jobs.triggerer_job_runner.stats.initialize")
    @patch.object(TriggerRunnerSupervisor, "start")
    def test_stats_initialize_called_on_execute(self, mock_supervisor_start, stats_init_mock, session):
        """Test that stats.initialize() is called when TriggererJobRunner._execute() is executed."""
        # Setup mock supervisor to immediately stop
        mock_supervisor = MagicMock()
        mock_supervisor.stop = False
        mock_supervisor._exit_code = None
        mock_supervisor.is_alive.return_value = True
        mock_supervisor.run.side_effect = lambda: setattr(mock_supervisor, "stop", True)
        mock_supervisor_start.return_value = mock_supervisor

        job = Job()
        session.add(job)
        session.flush()

        job_runner = TriggererJobRunner(job)
        job_runner.trigger_runner = mock_supervisor
        mock_supervisor.stop = True  # Stop immediately

        # We don't need to run the full _execute, just verify stats.initialize is called
        # before TriggerRunnerSupervisor.start
        with patch.object(job_runner, "register_signals"):
            # We expect this to fail since we're mocking
            with contextlib.suppress(Exception):
                job_runner._execute()

        # Verify stats.initialize was called with the expected configuration parameters
        stats_init_mock.assert_called_once()
        call_kwargs = stats_init_mock.call_args.kwargs
        assert "factory" in call_kwargs


class TestTriggererMessageTypes:
    def test_message_types_in_triggerer(self):
        """
        Test that ToSupervisor is a superset of ToTriggerSupervisor and ToTask is a superset of ToTriggerRunner.

        This test ensures that when new message types are added to ToSupervisor or ToTask,
        they are also properly handled in ToTriggerSupervisor and ToTriggerSupervisor.
        """

        def get_type_names(union_type):
            union_args = typing.get_args(union_type.__args__[0])
            return {arg.__name__ for arg in union_args}

        supervisor_types = get_type_names(ToSupervisor)
        task_types = get_type_names(ToTask)

        trigger_supervisor_types = get_type_names(ToTriggerSupervisor)
        trigger_runner_types = get_type_names(ToTriggerRunner)

        in_supervisor_but_not_in_trigger_supervisor = {
            "DeferTask",
            "GetAssetByName",
            "GetAssetByUri",
            "GetAssetsByAlias",
            "GetAssetEventByAsset",
            "GetAssetEventByAssetAlias",
            "GetDagRun",
            "GetPrevSuccessfulDagRun",
            "GetPreviousDagRun",
            "GetTaskBreadcrumbs",
            "GetTaskRescheduleStartDate",
            "GetXComCount",
            "GetXComSequenceItem",
            "GetXComSequenceSlice",
            "RescheduleTask",
            "RetryTask",
            "SetRenderedFields",
            "SkipDownstreamTasks",
            "SucceedTask",
            "ValidateInletsAndOutlets",
            "TaskState",
            "TriggerDagRun",
            "ResendLoggingFD",
            "CreateHITLDetailPayload",
            "SetRenderedMapIndex",
            "GetDag",
            # AIP-103 task/asset store — triggerer has no task execution context.
            "GetTaskStore",
            "SetTaskStore",
            "DeleteTaskStore",
            "ClearTaskStore",
            "GetAssetStoreByName",
            "GetAssetStoreByUri",
            "SetAssetStoreByName",
            "SetAssetStoreByUri",
            "DeleteAssetStoreByName",
            "DeleteAssetStoreByUri",
            "ClearAssetStoreByName",
            "ClearAssetStoreByUri",
        }

        in_task_but_not_in_trigger_runner = {
            "AssetResult",
            "AssetsByAliasResult",
            "AssetEventsResult",
            "DagRunResult",
            "SentFDs",
            "StartupDetails",
            "TaskBreadcrumbsResult",
            "TaskRescheduleStartDate",
            "InactiveAssetsResult",
            "CreateHITLDetailPayload",
            "PrevSuccessfulDagRunResult",
            "XComCountResponse",
            "XComSequenceIndexResult",
            "XComSequenceSliceResult",
            "PreviousDagRunResult",
            "PreviousTIResult",
            "HITLDetailRequestResult",
            "DagResult",
            # AIP-103 task/asset store results — worker-only responses to the above messages.
            "TaskStoreResult",
            "AssetStoreResult",
        }

        supervisor_diff = (
            supervisor_types - trigger_supervisor_types - in_supervisor_but_not_in_trigger_supervisor
        )
        task_diff = task_types - trigger_runner_types - in_task_but_not_in_trigger_runner

        assert not supervisor_diff, (
            f"New message types in ToSupervisor not handled in ToTriggerSupervisor: "
            f"{len(supervisor_diff)} types found:\n"
            + "\n".join(f"  - {t}" for t in sorted(supervisor_diff))
            + "\n\nEither handle these types in ToTriggerSupervisor or update in_supervisor_but_not_in_trigger_supervisor list."
        )

        assert not task_diff, (
            f"New message types in ToTask not handled in ToTriggerRunner: "
            f"{len(task_diff)} types found:\n"
            + "\n".join(f"  - {t}" for t in sorted(task_diff))
            + "\n\nEither handle these types in ToTriggerRunner or update in_task_but_not_in_trigger_runner list."
        )


class TestMakeTriggerSpan:
    """Tests for the _make_trigger_span helper in the triggerer job runner."""

    @pytest.fixture(autouse=True)
    def sdk_tracer_provider(self):
        self.exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(self.exporter))
        test_tracer = provider.get_tracer("test")
        with mock.patch("airflow.jobs.triggerer_job_runner.tracer", test_tracer):
            yield

    def _make_ti_dto(self, task_id="my_task", map_index=-1, context_carrier=None):
        return TaskInstanceDTO(
            id=uuid.uuid4(),
            dag_version_id=uuid.uuid4(),
            task_id=task_id,
            dag_id="test_dag",
            run_id="test_run",
            try_number=1,
            map_index=map_index,
            pool_slots=1,
            queue="default",
            priority_weight=1,
            context_carrier=context_carrier,
        )

    def test_make_trigger_span_name_with_task_instance(self):
        ti = self._make_ti_dto(task_id="sensor_task", map_index=-1)
        with _make_trigger_span(ti=ti, trigger_id=1, name="MySensor"):
            pass
        assert self.exporter.get_finished_spans()[0].name == "trigger.sensor_task"

    def test_make_trigger_span_name_with_mapped_task(self):
        ti = self._make_ti_dto(task_id="sensor_task", map_index=2)
        with _make_trigger_span(ti=ti, trigger_id=1, name="MySensor"):
            pass
        assert self.exporter.get_finished_spans()[0].name == "trigger.sensor_task_2"

    def test_make_trigger_span_name_without_task_instance(self):
        with _make_trigger_span(ti=None, trigger_id=42, name="Some trigger name"):
            pass
        assert self.exporter.get_finished_spans()[0].name == "trigger.Some trigger name"

    def test_make_trigger_span_uses_task_context_carrier(self):
        # Build a valid ti carrier from a separate provider so we have a known parent span.
        setup_provider = TracerProvider()
        setup_tracer = setup_provider.get_tracer("setup")
        parent_span = setup_tracer.start_span("ti_parent")
        parent_ctx = otel_trace.set_span_in_context(parent_span)
        ti_carrier: dict = {}
        TraceContextTextMapPropagator().inject(ti_carrier, context=parent_ctx)
        expected_parent_span_id = parent_span.get_span_context().span_id

        ti = self._make_ti_dto(context_carrier=ti_carrier)
        with _make_trigger_span(ti=ti, trigger_id=1, name="MySensor"):
            pass

        spans = self.exporter.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].parent is not None
        assert spans[0].parent.span_id == expected_parent_span_id

    def test_make_trigger_span_sets_attributes_with_ti(self):
        ti = self._make_ti_dto(task_id="my_task", map_index=1)
        with _make_trigger_span(ti=ti, trigger_id=5, name="MyTrigger"):
            pass

        attrs = self.exporter.get_finished_spans()[0].attributes
        assert attrs["airflow.trigger.name"] == "MyTrigger"
        assert attrs["airflow.dag_id"] == "test_dag"
        assert attrs["airflow.task_id"] == "my_task"
        assert attrs["airflow.dag_run.run_id"] == "test_run"
        assert attrs["airflow.task_instance.try_number"] == 1
        assert attrs["airflow.task_instance.map_index"] == 1

    def test_make_trigger_span_sets_only_trigger_name_without_ti(self):
        with _make_trigger_span(ti=None, trigger_id=99, name="OnlyTrigger"):
            pass

        attrs = self.exporter.get_finished_spans()[0].attributes
        assert attrs["airflow.trigger.name"] == "OnlyTrigger"
        assert "airflow.dag_id" not in attrs
        assert "airflow.task_id" not in attrs


def _read_frame_sync(sock) -> _RequestFrame | None:
    """Read a length-prefixed msgpack frame from a blocking socket."""
    lb = b""
    while len(lb) < 4:
        chunk = sock.recv(4 - len(lb))
        if not chunk:
            return None
        lb += chunk
    n = int.from_bytes(lb, "big")
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return msgspec.msgpack.decode(data, type=_RequestFrame)


@pytest_asyncio.fixture
async def decoder_pair():
    """Yield (decoder, server_sock). Caller owns closing."""
    server_sock, client_sock = socketpair()
    reader, writer = await asyncio.open_connection(sock=client_sock)
    decoder = TriggerCommsDecoder(async_writer=writer, async_reader=reader, socket=client_sock)
    await decoder.start_reader()
    yield decoder, server_sock
    if decoder._reader_task:
        if not decoder._reader_task.done():
            decoder._reader_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await decoder._reader_task
    writer.close()
    server_sock.close()


@pytest.mark.asyncio
@pytest.mark.execution_timeout(15)
async def test_all_send_paths_concurrent(decoder_pair):
    """
    All four send() paths running concurrently with responses returned out of order:

      1. asend() directly from async code           — pure-async path
      2. send() via asyncio.to_thread()              — mirrors apache/airflow#63913:
                                                       sync_to_async(hook_class)() → get_connection()
                                                       → SUPERVISOR_COMMS.send() from a thread pool thread
      3. send() from the event-loop thread           — mirrors apache/airflow#63760:
         via greenback                                 async_to_sync raised RuntimeError in same thread
      4. async_to_sync(asend)() from a thread        — trigger code that wraps an async fn which
                                                       internally calls asend; bridges via wrap_future

    The concurrent mix with shuffled responses also covers apache/airflow#65286: the
    _thread_lock + async_to_sync approach stalled the triggerer under this exact load pattern.
    """
    decoder, server_sock = decoder_pair
    N = 5
    N_TOTAL = N * 4

    def supervisor():
        frames = []
        for _ in range(N_TOTAL):
            f = _read_frame_sync(server_sock)
            if f is None:
                break
            frames.append(f)
        random.shuffle(frames)
        for f in frames:
            server_sock.sendall(
                _ResponseFrame(
                    id=f.id,
                    body={"type": "TriggerStateSync", "to_create": [], "to_cancel": []},
                ).as_bytes()
            )

    sup = threading.Thread(target=supervisor, daemon=True)
    sup.start()

    async def async_send(idx):
        return await decoder.asend(messages.TriggerStateChanges(events=None, finished=[idx], failures=None))

    async def from_thread_send(idx):
        # In production this path is taken by asgiref's own thread pool (sync_to_async),
        # which is invisible to asyncio's default executor.  We avoid asyncio.to_thread()
        # here because on Python < 3.12 loop.shutdown_default_executor() has no timeout
        # and hangs if any executor threads are still alive at loop teardown.
        # TODO: simplify with asyncio.to_thread() when Python 3.12 is the minimum.
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[messages.TriggerStateSync] = loop.create_future()

        def sync_send():
            try:
                result = decoder.send(
                    messages.TriggerStateChanges(events=None, finished=[N + idx], failures=None)
                )
                loop.call_soon_threadsafe(fut.set_result, result)
            except Exception as exc:
                loop.call_soon_threadsafe(fut.set_exception, exc)

        threading.Thread(target=sync_send, daemon=True).start()
        return await fut

    async def greenback_send(idx):
        await greenback.ensure_portal()
        return decoder.send(messages.TriggerStateChanges(events=None, finished=[2 * N + idx], failures=None))

    async def async_to_sync_send(idx):
        # Same executor-avoidance reason as from_thread_send above.
        # TODO: simplify with asyncio.to_thread() when Python 3.12 is the minimum.
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[messages.TriggerStateSync] = loop.create_future()

        def thread_fn():
            try:
                result = async_to_sync(decoder.asend)(
                    messages.TriggerStateChanges(events=None, finished=[3 * N + idx], failures=None)
                )
                loop.call_soon_threadsafe(fut.set_result, result)
            except Exception as exc:
                loop.call_soon_threadsafe(fut.set_exception, exc)

        threading.Thread(target=thread_fn, daemon=True).start()
        return await fut

    results = await asyncio.gather(
        *[asyncio.create_task(async_send(i)) for i in range(N)],
        *[asyncio.create_task(from_thread_send(i)) for i in range(N)],
        *[asyncio.create_task(greenback_send(i)) for i in range(N)],
        *[asyncio.create_task(async_to_sync_send(i)) for i in range(N)],
        return_exceptions=True,
    )

    sup.join(timeout=5)

    errors = [r for r in results if isinstance(r, Exception)]
    assert not errors, f"errors: {errors}"
    assert len(results) == N_TOTAL
    assert all(isinstance(r, messages.TriggerStateSync) for r in results)


@pytest.mark.asyncio
async def test_connection_close_cancels_pending(decoder_pair):
    """When the connection closes while asend() is awaiting, the future is cancelled."""
    decoder, server_sock = decoder_pair

    task = asyncio.create_task(
        decoder.asend(messages.TriggerStateChanges(events=None, finished=[1], failures=None))
    )
    await asyncio.sleep(0)

    server_sock.close()

    with pytest.raises((asyncio.CancelledError, Exception)):
        await asyncio.wait_for(task, timeout=5)


@pytest.mark.asyncio
async def test_unknown_frame_id_doesnt_crash_reader(decoder_pair):
    """An orphan response frame (no matching pending future) is silently dropped; reader stays alive."""
    decoder, server_sock = decoder_pair

    server_sock.sendall(
        _ResponseFrame(
            id=99999,
            body={"type": "TriggerStateSync", "to_create": [], "to_cancel": []},
        ).as_bytes()
    )

    await asyncio.sleep(0.05)

    assert decoder._reader_task is not None
    assert not decoder._reader_task.done(), "reader loop crashed unexpectedly"
