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

from unittest import mock

import structlog
import structlog.testing
from uuid6 import uuid7

from airflow.sdk import log as sdk_log


def _make_ti():
    ti = mock.MagicMock()
    ti.id = uuid7()
    return ti


def _make_logger():
    """Build a FilteringBoundLogger-like object exposing ``_logger``."""
    logger = mock.MagicMock()
    logger._logger = mock.MagicMock()
    return logger


class TestUploadToRemote:
    def test_warns_when_handler_unavailable(self):
        ti = _make_ti()
        with (
            mock.patch.object(sdk_log, "load_remote_log_handler", return_value=None),
            structlog.testing.capture_logs() as captured,
        ):
            sdk_log.upload_to_remote(_make_logger(), ti)

        events = [e for e in captured if e["event"] == "remote_log_handler_unavailable"]
        assert len(events) == 1
        assert events[0]["log_level"] == "warning"
        assert events[0]["ti_id"] == str(ti.id)

    def test_warns_when_path_resolution_fails(self):
        ti = _make_ti()
        handler = mock.MagicMock()
        boom = RuntimeError("cannot resolve path")
        with (
            mock.patch.object(sdk_log, "load_remote_log_handler", return_value=handler),
            mock.patch.object(sdk_log, "relative_path_from_logger", side_effect=boom),
            structlog.testing.capture_logs() as captured,
        ):
            sdk_log.upload_to_remote(_make_logger(), ti)

        events = [e for e in captured if e["event"] == "remote_log_path_resolution_failed"]
        assert len(events) == 1
        assert events[0]["log_level"] == "warning"
        assert events[0]["ti_id"] == str(ti.id)
        assert events[0]["exc_info"] is boom
        handler.upload.assert_not_called()

    def test_warns_when_upload_fails(self, tmp_path):
        ti = _make_ti()
        handler = mock.MagicMock()
        boom = RuntimeError("s3 unreachable")
        handler.upload.side_effect = boom
        relative = tmp_path / "dag_id" / "run_id" / "task.log"
        with (
            mock.patch.object(sdk_log, "load_remote_log_handler", return_value=handler),
            mock.patch.object(sdk_log, "relative_path_from_logger", return_value=relative),
            structlog.testing.capture_logs() as captured,
        ):
            sdk_log.upload_to_remote(_make_logger(), ti)

        events = [e for e in captured if e["event"] == "remote_log_upload_failed"]
        assert len(events) == 1
        assert events[0]["log_level"] == "warning"
        assert events[0]["ti_id"] == str(ti.id)
        assert events[0]["log_relative_path"] == relative.as_posix()
        assert events[0]["exc_info"] is boom
        handler.upload.assert_called_once_with(relative.as_posix(), ti)

    def test_silent_when_relative_path_is_none(self):
        ti = _make_ti()
        handler = mock.MagicMock()
        with (
            mock.patch.object(sdk_log, "load_remote_log_handler", return_value=handler),
            mock.patch.object(sdk_log, "relative_path_from_logger", return_value=None),
            structlog.testing.capture_logs() as captured,
        ):
            sdk_log.upload_to_remote(_make_logger(), ti)

        assert captured == []
        handler.upload.assert_not_called()

    def test_silent_on_success(self, tmp_path):
        ti = _make_ti()
        handler = mock.MagicMock()
        relative = tmp_path / "dag_id" / "run_id" / "task.log"
        with (
            mock.patch.object(sdk_log, "load_remote_log_handler", return_value=handler),
            mock.patch.object(sdk_log, "relative_path_from_logger", return_value=relative),
            structlog.testing.capture_logs() as captured,
        ):
            sdk_log.upload_to_remote(_make_logger(), ti)

        assert captured == []
        handler.upload.assert_called_once_with(relative.as_posix(), ti)


class TestConfigureLogging:
    def test_remote_processors_injected_after_dictconfig(self):
        """
        Regression test: remote processor injection must happen AFTER dictConfig() runs.

        dictConfig()'s non-incremental reset closes every handler in
        logging._handlerList. If the remote handler is built before dictConfig
        runs, it is closed before any task log is emitted and silently drops
        all records.
        """
        import airflow.sdk._shared.logging as shared_logging

        call_order = []

        mock_handler = mock.MagicMock()
        mock_handler.processors = (mock.MagicMock(),)

        def track_load_remote():
            call_order.append("load_remote_log_handler")
            return mock_handler

        original_inner = shared_logging.configure_logging

        def track_inner_configure(*args, **kwargs):
            call_order.append("dictConfig")
            return original_inner(*args, **kwargs)

        # configure_logging is @cache decorated — clear it so the test actually runs the function
        sdk_log.configure_logging.cache_clear()

        with (
            mock.patch.object(sdk_log, "load_remote_log_handler", side_effect=track_load_remote),
            mock.patch.object(shared_logging, "configure_logging", side_effect=track_inner_configure),
        ):
            sdk_log.configure_logging()

        assert "dictConfig" in call_order, "inner configure_logging was never called"
        assert "load_remote_log_handler" in call_order, "load_remote_log_handler() was never called"
        dictconfig_pos = call_order.index("dictConfig")
        load_remote_pos = call_order.index("load_remote_log_handler")
        assert dictconfig_pos < load_remote_pos, (
            "load_remote_log_handler() must be called AFTER dictConfig() runs, "
            "otherwise dictConfig closes the just-built handler before any task log is emitted"
        )
