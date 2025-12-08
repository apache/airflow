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

import logging
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import TYPE_CHECKING, cast
from unittest import mock
from unittest.mock import MagicMock, PropertyMock

import pendulum
import pytest
import time_machine
from kubernetes.client.rest import ApiException
from urllib3.exceptions import HTTPError as BaseHTTPError

from airflow.providers.cncf.kubernetes.exceptions import KubernetesApiError
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    AsyncPodManager,
    PodLogsConsumer,
    PodManager,
    PodPhase,
    parse_log_line,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.timezone import utc

from unit.cncf.kubernetes.test_callbacks import MockKubernetesPodOperatorCallback, MockWrapper

if TYPE_CHECKING:
    from pendulum import DateTime


def test_parse_log_line():
    log_message = "This should return no timestamp"
    timestamp, line = parse_log_line(log_message)
    assert timestamp is None
    assert line == log_message

    real_timestamp = "2020-10-08T14:16:17.793417674Z"
    timestamp, line = parse_log_line(f"{real_timestamp} {log_message}")
    assert timestamp == pendulum.parse(real_timestamp)
    assert line == log_message


class TestPodManager:
    def setup_method(self):
        self.mock_kube_client = mock.Mock()
        self.pod_manager = PodManager(
            kube_client=self.mock_kube_client,
            callbacks=[MockKubernetesPodOperatorCallback],
        )

    def test_read_pod_logs_successfully_returns_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.sentinel.logs
        logs = self.pod_manager.read_pod_logs(pod=mock.sentinel, container_name="base")
        assert isinstance(logs, PodLogsConsumer)
        assert logs.response == mock.sentinel.logs

    def test_read_pod_logs_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError("Boom"),
            mock.sentinel.logs,
        ]
        logs = self.pod_manager.read_pod_logs(pod=mock.sentinel, container_name="base")
        assert isinstance(logs, PodLogsConsumer)
        assert mock.sentinel.logs == logs.response
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls(
            [
                mock.call(
                    _preload_content=False,
                    container="base",
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                ),
                mock.call(
                    _preload_content=False,
                    container="base",
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                ),
            ]
        )

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.container_is_running")
    def test_fetch_container_logs_do_not_log_none(self, mock_container_is_running, caplog):
        MockWrapper.reset()
        caplog.set_level(logging.INFO)

        def consumer_iter():
            """This will simulate a container that hasn't produced any logs in the last read_timeout window"""
            yield from ()

        with mock.patch.object(PodLogsConsumer, "__iter__") as mock_consumer_iter:
            mock_consumer_iter.side_effect = consumer_iter
            mock_container_is_running.side_effect = [True, True, False]
            self.pod_manager.fetch_container_logs(mock.MagicMock(), "container-name", follow=True)
            assert "[container-name] None" not in (record.message for record in caplog.records)

    def test_read_pod_logs_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
        ]
        with pytest.raises(BaseHTTPError):
            self.pod_manager.read_pod_logs(pod=mock.sentinel, container_name="base")

    def test_read_pod_logs_successfully_with_tail_lines(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [mock.sentinel.logs]
        logs = self.pod_manager.read_pod_logs(pod=mock.sentinel, container_name="base", tail_lines=100)
        assert isinstance(logs, PodLogsConsumer)
        assert mock.sentinel.logs == logs.response
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls(
            [
                mock.call(
                    _preload_content=False,
                    container="base",
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                    tail_lines=100,
                ),
            ]
        )

    def test_read_pod_logs_successfully_with_since_seconds(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod_log.side_effect = [mock.sentinel.logs]
        logs = self.pod_manager.read_pod_logs(mock.sentinel, "base", since_seconds=2)
        assert isinstance(logs, PodLogsConsumer)
        assert mock.sentinel.logs == logs.response
        self.mock_kube_client.read_namespaced_pod_log.assert_has_calls(
            [
                mock.call(
                    _preload_content=False,
                    container="base",
                    follow=True,
                    timestamps=False,
                    name=mock.sentinel.metadata.name,
                    namespace=mock.sentinel.metadata.namespace,
                    since_seconds=2,
                ),
            ]
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_watch_pod_events(self, mock_time_sleep):
        events = mock.MagicMock()
        events.items = []
        for id in ["event 1", "event 2"]:
            event = mock.MagicMock()
            event.message = f"test {id}"
            event.involved_object.field_path = f"object {id}"
            events.items.append(event)
        startup_check_interval = 10

        def mock_read_pod_events(pod):
            self.pod_manager.stop_watching_events = True
            return events

        with (
            mock.patch.object(self.pod_manager, "read_pod_events", side_effect=mock_read_pod_events),
            mock.patch(
                "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.log",
                new_callable=PropertyMock,
            ) as log_mock,
        ):
            await self.pod_manager.watch_pod_events(pod=None, check_interval=startup_check_interval)

            log_mock.return_value.info.assert_any_call(
                "The Pod has an Event: %s from %s", "test event 1", "object event 1"
            )
            log_mock.return_value.info.assert_any_call(
                "The Pod has an Event: %s from %s", "test event 2", "object event 2"
            )
            mock_time_sleep.assert_called_once_with(startup_check_interval)

    def test_read_pod_events_successfully_returns_events(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.return_value = mock.sentinel.events
        events = self.pod_manager.read_pod_events(mock.sentinel)
        assert mock.sentinel.events == events

    def test_read_pod_events_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.side_effect = [
            BaseHTTPError("Boom"),
            mock.sentinel.events,
        ]
        events = self.pod_manager.read_pod_events(mock.sentinel)
        assert mock.sentinel.events == events
        self.mock_kube_client.list_namespaced_event.assert_has_calls(
            [
                mock.call(
                    namespace=mock.sentinel.metadata.namespace,
                    field_selector=f"involvedObject.name={mock.sentinel.metadata.name}",
                ),
                mock.call(
                    namespace=mock.sentinel.metadata.namespace,
                    field_selector=f"involvedObject.name={mock.sentinel.metadata.name}",
                ),
            ]
        )

    def test_read_pod_events_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.list_namespaced_event.side_effect = [
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
        ]
        with pytest.raises(AirflowException):
            self.pod_manager.read_pod_events(mock.sentinel)

    def test_read_pod_returns_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.return_value = mock.sentinel.pod_info
        pod_info = self.pod_manager.read_pod(mock.sentinel)
        assert mock.sentinel.pod_info == pod_info

    def test_read_pod_retries_successfully(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.side_effect = [
            BaseHTTPError("Boom"),
            mock.sentinel.pod_info,
        ]
        pod_info = self.pod_manager.read_pod(mock.sentinel)
        assert mock.sentinel.pod_info == pod_info
        self.mock_kube_client.read_namespaced_pod.assert_has_calls(
            [
                mock.call(mock.sentinel.metadata.name, mock.sentinel.metadata.namespace),
                mock.call(mock.sentinel.metadata.name, mock.sentinel.metadata.namespace),
            ]
        )

    def test_monitor_pod_empty_logs(self):
        mock.sentinel.metadata = mock.MagicMock()
        running_status = mock.MagicMock()
        running_status.configure_mock(**{"name": "base", "state.running": True})
        pod_info_running = mock.MagicMock(**{"status.container_statuses": [running_status]})
        pod_info_succeeded = mock.MagicMock(**{"status.phase": PodPhase.SUCCEEDED})

        def pod_state_gen():
            yield pod_info_running
            while True:
                yield pod_info_succeeded

        self.mock_kube_client.read_namespaced_pod.side_effect = pod_state_gen()
        mock_response = mock.MagicMock(stream=mock.MagicMock(return_value=iter(())))
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock_response
        self.pod_manager.fetch_container_logs(mock.sentinel, "base")

    def test_monitor_pod_logs_failures_non_fatal(self):
        mock.sentinel.metadata = mock.MagicMock()
        running_status = mock.MagicMock()
        running_status.configure_mock(**{"name": "base", "state.running": True})
        pod_info_running = mock.MagicMock(**{"status.container_statuses": [running_status]})
        pod_info_succeeded = mock.MagicMock(**{"status.phase": PodPhase.SUCCEEDED})

        def pod_state_gen():
            yield pod_info_running
            yield pod_info_running
            while True:
                yield pod_info_succeeded

        self.mock_kube_client.read_namespaced_pod.side_effect = pod_state_gen()

        def pod_log_gen():
            while True:
                yield BaseHTTPError("Boom")

        self.mock_kube_client.read_namespaced_pod_log.side_effect = pod_log_gen()

        self.pod_manager.fetch_container_logs(mock.sentinel, "base")

    def test_read_pod_retries_fails(self):
        mock.sentinel.metadata = mock.MagicMock()
        self.mock_kube_client.read_namespaced_pod.side_effect = [
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
            BaseHTTPError("Boom"),
        ]
        with pytest.raises(AirflowException):
            self.pod_manager.read_pod(mock.sentinel)

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.container_is_running")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.read_pod_logs")
    def test_fetch_container_logs_returning_last_timestamp(
        self, mock_read_pod_logs, mock_container_is_running
    ):
        timestamp_string = "2020-10-08T14:16:17.793417674Z"
        mock_read_pod_logs.return_value = [bytes(f"{timestamp_string} message", "utf-8"), b"notimestamp"]
        mock_container_is_running.side_effect = [True, False]

        status = self.pod_manager.fetch_container_logs(mock.MagicMock(), mock.MagicMock(), follow=True)

        assert status.last_log_time == cast("DateTime", pendulum.parse(timestamp_string))

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.container_is_running")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.read_pod_logs")
    def test_fetch_container_logs_invoke_progress_callback(
        self, mock_read_pod_logs, mock_container_is_running
    ):
        MockWrapper.reset()
        mock_callbacks = MockWrapper.mock_callbacks
        message = "2020-10-08T14:16:17.793417674Z message"
        no_ts_message = "notimestamp"
        mock_read_pod_logs.return_value = [bytes(message, "utf-8"), bytes(no_ts_message, "utf-8")]
        mock_container_is_running.return_value = False

        self.pod_manager.fetch_container_logs(mock.MagicMock(), mock.MagicMock(), follow=True)
        mock_callbacks.progress_callback.assert_has_calls(
            [
                mock.call(line=message, client=self.pod_manager._client, mode="sync"),
                mock.call(line=no_ts_message, client=self.pod_manager._client, mode="sync"),
            ]
        )

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.container_is_running")
    def test_fetch_container_logs_failures(self, mock_container_is_running):
        MockWrapper.reset()
        mock_callbacks = MockWrapper.mock_callbacks
        last_timestamp_string = "2020-10-08T14:18:17.793417674Z"
        messages = [
            bytes("2020-10-08T14:16:17.793417674Z message", "utf-8"),
            bytes("2020-10-08T14:17:17.793417674Z message", "utf-8"),
            None,
            bytes(f"{last_timestamp_string} message", "utf-8"),
        ]
        expected_call_count = len([message for message in messages if message is not None])

        def consumer_iter():
            while messages:
                message = messages.pop(0)
                if message is None:
                    raise BaseHTTPError("Boom")
                yield message

        with mock.patch.object(PodLogsConsumer, "__iter__") as mock_consumer_iter:
            mock_consumer_iter.side_effect = consumer_iter
            mock_container_is_running.side_effect = [True, True, False]
            status = self.pod_manager.fetch_container_logs(mock.MagicMock(), mock.MagicMock(), follow=True)
        assert status.last_log_time == cast("DateTime", pendulum.parse(last_timestamp_string))
        assert mock_callbacks.progress_callback.call_count == expected_call_count

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.container_is_running")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.read_pod_logs")
    def test_parse_multi_line_logs(self, mock_read_pod_logs, mock_container_is_running, caplog):
        log = (
            "2020-10-08T14:16:17.793417674Z message1 line1\n"
            "message1 line2\n"
            "message1 line3\n"
            "2020-10-08T14:16:18.793417674Z message2 line1\n"
            "message2 line2\n"
            "2020-10-08T14:16:19.793417674Z message3 line1\n"
        )
        mock_read_pod_logs.return_value = [bytes(log_line, "utf-8") for log_line in log.split("\n")]
        mock_container_is_running.return_value = False

        with caplog.at_level(logging.INFO):
            self.pod_manager.fetch_container_logs(mock.MagicMock(), mock.MagicMock(), follow=True)

        assert "message1 line1" in caplog.text
        assert "message1 line2" in caplog.text
        assert "message1 line3" in caplog.text
        assert "message2 line1" in caplog.text
        assert "message2 line2" in caplog.text
        assert "message3 line1" in caplog.text
        assert "ERROR" not in caplog.text

    @pytest.mark.parametrize("status", [409, 429])
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.run_pod_async")
    def test_start_pod_retries_on_409_or_429_error(self, mock_run_pod_async, status):
        mock_run_pod_async.side_effect = [
            ApiException(status=status),
            mock.MagicMock(),
        ]
        self.pod_manager.create_pod(mock.sentinel)
        assert mock_run_pod_async.call_count == 2

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.run_pod_async")
    def test_start_pod_fails_on_other_exception(self, mock_run_pod_async):
        mock_run_pod_async.side_effect = [ApiException(status=401)]
        with pytest.raises(ApiException):
            self.pod_manager.create_pod(mock.sentinel)

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.run_pod_async")
    def test_start_pod_retries_three_times(self, mock_run_pod_async):
        mock_run_pod_async.side_effect = [
            ApiException(status=409),
            ApiException(status=409),
            ApiException(status=409),
            ApiException(status=409),
            ApiException(status=409),
            ApiException(status=409),
        ]
        with pytest.raises(ApiException):
            self.pod_manager.create_pod(mock.sentinel)

        assert mock_run_pod_async.call_count == 5

    @pytest.mark.asyncio
    async def test_start_pod_raises_informative_error_on_scheduled_timeout(self):
        pod_response = mock.MagicMock()
        pod_response.status.phase = "Pending"
        self.mock_kube_client.read_namespaced_pod.return_value = pod_response
        expected_msg = "Pod took too long to be scheduled on the cluster, giving up. More than 0s. Check the pod events in kubernetes."
        mock_pod = MagicMock()
        with pytest.raises(AirflowException, match=expected_msg):
            await self.pod_manager.await_pod_start(
                pod=mock_pod,
                schedule_timeout=0,
                startup_timeout=0,
            )

    @pytest.mark.asyncio
    async def test_start_pod_raises_informative_error_on_startup_timeout(self):
        pod_response = mock.MagicMock()
        pod_response.status.phase = "Pending"
        condition = mock.MagicMock()
        condition.type = "PodScheduled"
        condition.status = "True"
        pod_response.status.conditions = [condition]

        self.mock_kube_client.read_namespaced_pod.return_value = pod_response
        expected_msg = "Pod took too long to start. More than 0s. Check the pod events in kubernetes."
        mock_pod = MagicMock()
        with pytest.raises(AirflowException, match=expected_msg):
            await self.pod_manager.await_pod_start(
                pod=mock_pod,
                schedule_timeout=0,
                startup_timeout=0,
            )

    @pytest.mark.asyncio
    async def test_start_pod_raises_fast_error_on_image_error(self):
        pod_response = mock.MagicMock()
        pod_response.status.phase = "Pending"
        container_statuse = mock.MagicMock()
        waiting_state = mock.MagicMock()
        waiting_state.reason = "ErrImagePull"
        waiting_state.message = "Test error"
        container_statuse.state.waiting = waiting_state
        pod_response.status.container_statuses = [container_statuse]

        self.mock_kube_client.read_namespaced_pod.return_value = pod_response
        expected_msg = f"Pod docker image cannot be pulled, unable to start: {waiting_state.reason}\n{waiting_state.message}"
        mock_pod = MagicMock()
        with pytest.raises(AirflowException, match=expected_msg):
            await self.pod_manager.await_pod_start(
                pod=mock_pod,
                schedule_timeout=60,
                startup_timeout=60,
            )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_start_pod_startup_interval_seconds(self, mock_time_sleep):
        condition_scheduled = mock.MagicMock()
        condition_scheduled.type = "PodScheduled"
        condition_scheduled.status = "True"

        pod_info_pending = mock.MagicMock(**{"status.phase": PodPhase.PENDING})
        pod_info_pending_scheduled = mock.MagicMock(
            **{"status.phase": PodPhase.PENDING, "status.conditions": [condition_scheduled]}
        )
        pod_info_succeeded = mock.MagicMock(**{"status.phase": PodPhase.SUCCEEDED})

        def pod_state_gen():
            yield pod_info_pending
            yield pod_info_pending_scheduled
            yield pod_info_pending_scheduled
            while True:
                yield pod_info_succeeded

        self.mock_kube_client.read_namespaced_pod.side_effect = pod_state_gen()
        startup_check_interval = 10  # Any value is fine, as time.sleep is mocked to do nothing
        schedule_timeout = 30
        startup_timeout = 60
        mock_pod = MagicMock()

        with mock.patch.object(self.pod_manager.log, "info") as mock_log_info:
            await self.pod_manager.await_pod_start(
                pod=mock_pod,
                schedule_timeout=schedule_timeout,  # Never hit, any value is fine, as time.sleep is mocked to do nothing
                startup_timeout=startup_timeout,  # Never hit, any value is fine, as time.sleep is mocked to do nothing
                check_interval=startup_check_interval,
            )
            mock_time_sleep.assert_called_with(startup_check_interval)
            assert self.pod_manager.stop_watching_events is True
            assert mock_time_sleep.call_count == 3
            mock_log_info.assert_any_call(
                "::group::Waiting until %ss to get the POD scheduled...", schedule_timeout
            )
            mock_log_info.assert_any_call("Waiting %ss to get the POD running...", startup_timeout)

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    def test_container_is_running(self, container_is_running_mock):
        mock_pod = MagicMock()
        self.pod_manager.read_pod = mock.MagicMock(return_value=mock_pod)
        self.pod_manager.container_is_running(None, "base")
        container_is_running_mock.assert_called_with(pod=mock_pod, container_name="base")

    @pytest.mark.parametrize("follow", [True, False])
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodLogsConsumer.logs_available")
    def test_fetch_container_done(self, logs_available, container_running, follow):
        """If container done, should exit, no matter setting of follow."""
        mock_pod = MagicMock()
        logs_available.return_value = False
        container_running.return_value = False
        ret = self.pod_manager.fetch_container_logs(pod=mock_pod, container_name="base", follow=follow)
        assert ret.last_log_time is None
        assert ret.running is False

    # adds all valid types for container_logs
    @pytest.mark.parametrize("follow", [True, False])
    @pytest.mark.parametrize("container_logs", ["base", "alpine", True, ["base", "alpine"]])
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    def test_fetch_requested_container_logs(self, container_is_running, container_logs, follow):
        mock_pod = MagicMock()
        self.pod_manager.read_pod = MagicMock()
        self.pod_manager.get_container_names = MagicMock()
        self.pod_manager.get_container_names.return_value = ["base", "alpine"]
        container_is_running.return_value = False
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.MagicMock(
            stream=mock.MagicMock(return_value=[b"2021-01-01 hi"])
        )

        ret_values = self.pod_manager.fetch_requested_container_logs(
            pod=mock_pod, containers=container_logs, follow_logs=follow
        )
        for ret in ret_values:
            assert ret.running is False

    # adds all invalid types for container_logs
    @pytest.mark.parametrize("container_logs", [1, None, 6.8, False])
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    def test_fetch_requested_container_logs_invalid(self, container_running, container_logs):
        mock_pod = MagicMock()
        self.pod_manager.read_pod = MagicMock()
        self.pod_manager.get_container_names = MagicMock()
        self.pod_manager.get_container_names.return_value = ["base", "alpine"]
        container_running.return_value = False
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.MagicMock(
            stream=mock.MagicMock(return_value=[b"2021-01-01 hi"])
        )

        ret_values = self.pod_manager.fetch_requested_container_logs(
            pod=mock_pod,
            containers=container_logs,
        )

        assert len(ret_values) == 0

    @mock.patch("pendulum.now")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodLogsConsumer.logs_available")
    def test_fetch_container_since_time(self, logs_available, container_running, mock_now):
        """If given since_time, should be used."""
        mock_pod = MagicMock()
        mock_now.return_value = pendulum.datetime(2020, 1, 1, 0, 0, 5, tz="UTC")
        logs_available.return_value = True
        container_running.return_value = False
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.MagicMock(
            stream=mock.MagicMock(return_value=[b"2021-01-01 hi"])
        )
        since_time = pendulum.datetime(2020, 1, 1, tz="UTC")
        self.pod_manager.fetch_container_logs(pod=mock_pod, container_name="base", since_time=since_time)
        args, kwargs = self.mock_kube_client.read_namespaced_pod_log.call_args_list[0]
        assert kwargs["since_seconds"] == 5

    @pytest.mark.parametrize(
        ("follow", "is_running_calls", "exp_running"), [(True, 3, False), (False, 3, False)]
    )
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    def test_fetch_container_running_follow(
        self, container_running_mock, follow, is_running_calls, exp_running
    ):
        """
        When called with follow, should keep looping even after disconnections, if pod still running.
        When called with follow=False, should return immediately even though still running.
        """
        mock_pod = MagicMock()
        container_running_mock.side_effect = [True, True, False]
        self.mock_kube_client.read_namespaced_pod_log.return_value = mock.MagicMock(
            stream=mock.MagicMock(return_value=[b"2021-01-01 hi"])
        )
        ret = self.pod_manager.fetch_container_logs(pod=mock_pod, container_name="base", follow=follow)
        assert len(container_running_mock.call_args_list) == is_running_calls
        assert ret.last_log_time == pendulum.datetime(2021, 1, 1, tz="UTC")
        assert ret.running is exp_running

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.kubernetes_stream")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.extract_xcom_kill")
    def test_extract_xcom_success(self, mock_exec_xcom_kill, mock_kubernetes_stream):
        """test when valid json is retrieved from xcom sidecar container."""
        xcom_json = """{"a": "true"}"""
        mock_pod = MagicMock()
        mock_client = MagicMock()
        mock_client.peek_stderr.return_value = ""
        mock_client.read_all.return_value = xcom_json
        mock_kubernetes_stream.return_value = mock_client
        ret = self.pod_manager.extract_xcom(pod=mock_pod)
        assert ret == xcom_json
        assert mock_exec_xcom_kill.call_count == 1

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.kubernetes_stream")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.extract_xcom_kill")
    def test_extract_xcom_failure(self, mock_exec_xcom_kill, mock_kubernetes_stream):
        """test when invalid json is retrieved from xcom sidecar container."""
        xcom_json = """{"a": "tru"""  # codespell:ignore tru
        mock_pod = MagicMock()
        mock_client = MagicMock()
        mock_client.peek_stderr.return_value = ""
        mock_client.read_all.return_value = xcom_json
        mock_kubernetes_stream.return_value = mock_client
        with pytest.raises(JSONDecodeError):
            self.pod_manager.extract_xcom(pod=mock_pod)
        assert mock_exec_xcom_kill.call_count == 1

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.kubernetes_stream")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.extract_xcom_kill")
    def test_extract_xcom_empty(self, mock_exec_xcom_kill, mock_kubernetes_stream):
        """test when __airflow_xcom_result_empty__ is retrieved from xcom sidecar container."""
        mock_pod = MagicMock()
        xcom_result = "__airflow_xcom_result_empty__"
        mock_client = MagicMock()
        mock_client.peek_stderr.return_value = ""
        mock_client.read_all.return_value = xcom_result
        mock_kubernetes_stream.return_value = mock_client
        ret = self.pod_manager.extract_xcom(pod=mock_pod)
        assert ret == xcom_result
        assert mock_exec_xcom_kill.call_count == 1

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.kubernetes_stream")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.extract_xcom_kill")
    def test_extract_xcom_none(self, mock_exec_xcom_kill, mock_kubernetes_stream):
        """test when None is retrieved from xcom sidecar container."""
        mock_pod = MagicMock()
        mock_client = MagicMock()
        mock_client.peek_stderr.return_value = ""
        mock_client.read_all.return_value = None
        mock_kubernetes_stream.return_value = mock_client
        with pytest.raises(AirflowException):
            self.pod_manager.extract_xcom(pod=mock_pod)
        assert mock_exec_xcom_kill.call_count == 1

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.container_is_terminated")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.container_is_running")
    def test_await_xcom_sidecar_container_timeout(
        self, mock_container_is_running, mock_container_is_terminated
    ):
        mock_pod = MagicMock()
        mock_container_is_running.return_value = False
        mock_container_is_terminated.return_value = False
        with pytest.raises(AirflowException):
            self.pod_manager.await_xcom_sidecar_container_start(pod=mock_pod, timeout=10, log_interval=5)
        mock_container_is_running.assert_any_call(mock_pod, "airflow-xcom-sidecar")
        mock_container_is_terminated.assert_any_call(mock_pod, "airflow-xcom-sidecar")

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.container_is_terminated")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.container_is_running")
    def test_await_xcom_sidecar_container_terminated(
        self, mock_container_is_running, mock_container_is_terminated
    ):
        mock_pod = MagicMock()
        mock_container_is_running.return_value = False
        mock_container_is_terminated.return_value = True
        match_error = (
            "Xcom sidecar container is already terminated! Not possible to read xcom output of task."
        )
        with pytest.raises(AirflowException, match=match_error):
            self.pod_manager.await_xcom_sidecar_container_start(pod=mock_pod, timeout=10, log_interval=5)

        mock_container_is_running.assert_any_call(mock_pod, "airflow-xcom-sidecar")
        mock_container_is_terminated.assert_any_call(mock_pod, "airflow-xcom-sidecar")

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.container_is_running")
    def test_await_xcom_sidecar_container_starts(self, mock_container_is_running):
        mock_pod = MagicMock()
        mock_container_is_running.return_value = True
        self.pod_manager.await_xcom_sidecar_container_start(pod=mock_pod)
        mock_container_is_running.assert_any_call(mock_pod, "airflow-xcom-sidecar")


class TestAsyncPodManager:
    @pytest.fixture
    def mock_log_info(self):
        with mock.patch.object(self.async_pod_manager.log, "info") as mock_log_info:
            yield mock_log_info

    def setup_method(self):
        self.mock_async_hook = mock.AsyncMock()
        self.async_pod_manager = AsyncPodManager(
            async_hook=self.mock_async_hook,
            callbacks=[],
        )

    @pytest.mark.asyncio
    async def test_start_pod_raises_informative_error_on_scheduled_timeout(self):
        pod_response = mock.MagicMock()
        pod_response.status.phase = "Pending"
        self.mock_async_hook.get_pod.return_value = pod_response
        expected_msg = "Pod took too long to be scheduled on the cluster, giving up. More than 0s. Check the pod events in kubernetes."
        mock_pod = mock.MagicMock()
        with pytest.raises(AirflowException, match=expected_msg):
            await self.async_pod_manager.await_pod_start(
                pod=mock_pod,
                schedule_timeout=0,
                startup_timeout=0,
            )
        self.mock_async_hook.get_pod.assert_called()

    @pytest.mark.asyncio
    async def test_start_pod_raises_informative_error_on_startup_timeout(self):
        pod_response = mock.MagicMock()
        pod_response.status.phase = "Pending"
        condition = mock.MagicMock()
        condition.type = "PodScheduled"
        condition.status = "True"
        pod_response.status.conditions = [condition]
        self.mock_async_hook.get_pod.return_value = pod_response
        expected_msg = "Pod took too long to start. More than 0s. Check the pod events in kubernetes."
        mock_pod = mock.MagicMock()
        with pytest.raises(AirflowException, match=expected_msg):
            await self.async_pod_manager.await_pod_start(
                pod=mock_pod,
                schedule_timeout=0,
                startup_timeout=0,
            )
        self.mock_async_hook.get_pod.assert_called()

    @pytest.mark.asyncio
    async def test_start_pod_raises_fast_error_on_image_error(self):
        pod_response = mock.MagicMock()
        pod_response.status.phase = "Pending"
        container_status = mock.MagicMock()
        waiting_state = mock.MagicMock()
        waiting_state.reason = "ErrImagePull"
        waiting_state.message = "Test error"
        container_status.state.waiting = waiting_state
        pod_response.status.container_statuses = [container_status]
        self.mock_async_hook.get_pod.return_value = pod_response
        expected_msg = f"Pod docker image cannot be pulled, unable to start: {waiting_state.reason}\n{waiting_state.message}"
        mock_pod = mock.MagicMock()
        with pytest.raises(AirflowException, match=expected_msg):
            await self.async_pod_manager.await_pod_start(
                pod=mock_pod,
                schedule_timeout=60,
                startup_timeout=60,
            )
        self.mock_async_hook.get_pod.assert_called()

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_start_pod_startup_interval_seconds(self, mock_time_sleep, mock_log_info):
        condition_scheduled = mock.MagicMock()
        condition_scheduled.type = "PodScheduled"
        condition_scheduled.status = "True"

        pod_info_pending = mock.MagicMock()
        pod_info_pending.status.phase = PodPhase.PENDING
        pod_info_pending.status.conditions = []

        pod_info_pending_scheduled = mock.MagicMock()
        pod_info_pending_scheduled.status.phase = PodPhase.PENDING
        pod_info_pending_scheduled.status.conditions = [condition_scheduled]

        pod_info_succeeded = mock.MagicMock()
        pod_info_succeeded.status.phase = PodPhase.SUCCEEDED

        # Simulate sequence of pod states
        self.mock_async_hook.get_pod.side_effect = [
            pod_info_pending,
            pod_info_pending_scheduled,
            pod_info_pending_scheduled,
            pod_info_succeeded,
        ]
        startup_check_interval = 10
        schedule_timeout = 30
        startup_timeout = 60
        mock_pod = mock.MagicMock()

        await self.async_pod_manager.await_pod_start(
            pod=mock_pod,
            schedule_timeout=schedule_timeout,
            startup_timeout=startup_timeout,
            check_interval=startup_check_interval,
        )
        assert mock_time_sleep.call_count == 3
        mock_log_info.assert_any_call(
            "::group::Waiting until %ss to get the POD scheduled...", schedule_timeout
        )
        mock_log_info.assert_any_call("Waiting %ss to get the POD running...", startup_timeout)
        assert self.async_pod_manager.stop_watching_events is True

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep", new_callable=mock.AsyncMock)
    async def test_watch_pod_events(self, mock_time_sleep, mock_log_info):
        mock_pod = mock.MagicMock()
        mock_pod.metadata.name = "test-pod"
        mock_pod.metadata.namespace = "default"

        events = mock.MagicMock()
        events.items = []
        for id in ["event 1", "event 2"]:
            event = mock.MagicMock()
            event.message = f"test {id}"
            event.involved_object.field_path = f"object {id}"
            events.items.append(event)
        startup_check_interval = 10

        def get_pod_events_side_effect(name, namespace):
            self.async_pod_manager.stop_watching_events = True
            return events

        self.mock_async_hook.get_pod_events.side_effect = get_pod_events_side_effect

        await self.async_pod_manager.watch_pod_events(pod=mock_pod, check_interval=startup_check_interval)
        mock_log_info.assert_any_call("The Pod has an Event: %s from %s", "test event 1", "object event 1")
        mock_log_info.assert_any_call("The Pod has an Event: %s from %s", "test event 2", "object event 2")
        mock_time_sleep.assert_called_once_with(startup_check_interval)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("log_lines", "now", "expected_log_messages", "not_expected_log_messages"),
        [
            # Case 1: No logs
            ([], pendulum.now(), [], []),
            # Case 2: One log line with timestamp before now
            (
                [f"{pendulum.now().subtract(seconds=2).to_iso8601_string()} message"],
                pendulum.now(),
                ["message"],
                [],
            ),
            # Case 3: Log line with timestamp equal to now (should be skipped, so last_time is None)
            ([f"{pendulum.now().to_iso8601_string()} message"], pendulum.now(), [], ["message"]),
            # Case 4: Multiple log lines, last before now
            (
                [
                    f"{pendulum.now().subtract(seconds=3).to_iso8601_string()} msg1",
                    f"{pendulum.now().subtract(seconds=2).to_iso8601_string()} msg2",
                ],
                pendulum.now(),
                ["msg1", "msg2"],
                [],
            ),
            # Case 5: Log lines with continuation (no timestamp)
            (
                [
                    f"{pendulum.now().subtract(seconds=2).to_iso8601_string()} msg1",
                    "continued line",
                ],
                pendulum.now(),
                ["msg1\ncontinued line"],
                [],
            ),
            # Case 6: Log lines with continuation (no timestamp)
            (
                [
                    f"{pendulum.now().subtract(seconds=2).to_iso8601_string()} msg1",
                    f"{pendulum.now().to_iso8601_string()} msg2",
                ],
                pendulum.now(),
                ["msg1"],
                ["msg2"],
            ),
        ],
    )
    async def test_fetch_container_logs_before_current_sec_various_logs(
        self, log_lines, now, expected_log_messages, not_expected_log_messages
    ):
        pod = mock.MagicMock()
        container_name = "base"
        since_time = now.subtract(minutes=1)
        mock_async_hook = mock.AsyncMock()
        mock_async_hook.read_logs.return_value = log_lines

        with mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.pendulum.now", return_value=now):
            async_pod_manager = AsyncPodManager(
                async_hook=mock_async_hook,
                callbacks=[],
            )
            with mock.patch.object(async_pod_manager.log, "info") as mock_log_info:
                result = await async_pod_manager.fetch_container_logs_before_current_sec(
                    pod=pod, container_name=container_name, since_time=since_time
                )
                assert result == now

                for expected in expected_log_messages:
                    mock_log_info.assert_any_call("[%s] %s", container_name, expected)
                for not_expected in not_expected_log_messages:
                    unexpected_call = mock.call("[%s] %s", container_name, not_expected)
                    assert unexpected_call not in mock_log_info.mock_calls

    @pytest.mark.asyncio
    async def test_fetch_container_logs_before_current_sec_error_handling(self):
        pod = mock.MagicMock()
        container_name = "base"
        since_time = pendulum.now().subtract(minutes=1)

        async def fake_read_logs(**kwargs):
            raise KubernetesApiError("error")

        self.async_pod_manager._hook.read_logs = fake_read_logs
        with pytest.raises(KubernetesApiError):
            await self.async_pod_manager.fetch_container_logs_before_current_sec(
                pod=pod, container_name=container_name, since_time=since_time
            )


class TestPodLogsConsumer:
    @pytest.mark.parametrize(
        ("chunks", "expected_logs"),
        [
            ([b"message"], [b"message"]),
            ([b"message1\nmessage2"], [b"message1\n", b"message2"]),
            ([b"message1\n", b"message2"], [b"message1\n", b"message2"]),
            ([b"first_part", b"_second_part"], [b"first_part_second_part"]),
            ([b""], [b""]),
        ],
    )
    def test_chunks(self, chunks, expected_logs):
        with mock.patch.object(PodLogsConsumer, "logs_available") as logs_available:
            logs_available.return_value = True
            consumer = PodLogsConsumer(
                response=mock.MagicMock(stream=mock.MagicMock(return_value=chunks)),
                pod=mock.MagicMock(),
                pod_manager=mock.MagicMock(container_is_running=mock.MagicMock(return_value=True)),
                container_name="base",
            )
            assert list(consumer) == expected_logs

    def test_container_is_not_running(self):
        with mock.patch.object(PodLogsConsumer, "logs_available") as logs_available:
            logs_available.return_value = False
            consumer = PodLogsConsumer(
                response=mock.MagicMock(stream=mock.MagicMock(return_value=[b"message1", b"message2"])),
                pod=mock.MagicMock(),
                pod_manager=mock.MagicMock(container_is_running=mock.MagicMock(return_value=False)),
                container_name="base",
            )
            assert list(consumer) == []

    @pytest.mark.parametrize(
        (
            "container_run",
            "termination_time",
            "now_time",
            "post_termination_timeout",
            "expected_logs_available",
        ),
        [
            (
                False,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 1, 0, 0, tzinfo=utc),
                120,
                True,
            ),
            (
                False,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 2, 0, 0, tzinfo=utc),
                120,
                False,
            ),
            (
                False,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 5, 0, 0, tzinfo=utc),
                120,
                False,
            ),
            (
                True,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 1, 0, 0, tzinfo=utc),
                120,
                True,
            ),
            (
                True,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 2, 0, 0, tzinfo=utc),
                120,
                True,
            ),
            (
                True,
                datetime(2022, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2022, 1, 1, 0, 5, 0, 0, tzinfo=utc),
                120,
                True,
            ),
        ],
    )
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.container_is_running")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.get_container_status")
    def test_logs_available(
        self,
        mock_get_container_status,
        mock_container_is_running,
        container_run,
        termination_time,
        now_time,
        post_termination_timeout,
        expected_logs_available,
    ):
        mock_container_is_running.return_value = container_run
        mock_get_container_status.return_value = mock.MagicMock(
            state=mock.MagicMock(terminated=mock.MagicMock(finished_at=termination_time))
        )
        with time_machine.travel(now_time):
            consumer = PodLogsConsumer(
                response=mock.MagicMock(),
                pod=mock.MagicMock(),
                pod_manager=mock.MagicMock(),
                container_name="base",
                post_termination_timeout=post_termination_timeout,
            )
            assert consumer.logs_available() == expected_logs_available

    @pytest.mark.parametrize(
        (
            "read_pod_cache_timeout",
            "mock_read_pod_at_0",
            "mock_read_pod_at_1",
            "mock_read_pods",
            "expected_read_pods",
        ),
        [
            (
                120,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 1, 0, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #0"],
            ),
            (
                120,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 2, 0, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #0"],
            ),
            (
                120,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 3, 0, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #1"],
            ),
            (
                2,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 0, 1, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #0"],
            ),
            (
                2,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 0, 2, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #0"],
            ),
            (
                2,
                datetime(2023, 1, 1, 0, 0, 0, 0, tzinfo=utc),
                datetime(2023, 1, 1, 0, 0, 3, 0, tzinfo=utc),
                ["Read pod #0", "Read pod #1"],
                ["Read pod #0", "Read pod #1"],
            ),
        ],
    )
    def test_read_pod(
        self,
        read_pod_cache_timeout,
        mock_read_pod_at_0,
        mock_read_pod_at_1,
        mock_read_pods,
        expected_read_pods,
    ):
        consumer = PodLogsConsumer(
            response=mock.MagicMock(),
            pod=mock.MagicMock(),
            pod_manager=mock.MagicMock(),
            container_name="base",
            read_pod_cache_timeout=read_pod_cache_timeout,
        )
        consumer.pod_manager.read_pod.side_effect = mock_read_pods
        # first read
        with time_machine.travel(mock_read_pod_at_0):
            assert consumer.read_pod() == expected_read_pods[0]

        # second read
        with time_machine.travel(mock_read_pod_at_1):
            assert consumer.read_pod() == expected_read_pods[1]
