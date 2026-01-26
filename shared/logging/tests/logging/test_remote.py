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

from airflow_shared.logging.remote import RemoteLogStreamIO, discover_remote_log_handler


class DummyRemoteLogIO:
    @property
    def processors(self):
        return ()

    def upload(self, path, ti):
        pass

    def read(self, relative_path, ti):
        return ([], [])


class TestDiscoverRemoteLogHandler:
    def test_discovers_handler_and_conn_id(self):
        handler = DummyRemoteLogIO()
        config = {"version": 1}

        mock_module = mock.MagicMock()
        mock_module.REMOTE_TASK_LOG = handler
        mock_module.DEFAULT_REMOTE_CONN_ID = "aws_default"

        with mock.patch("airflow_shared.logging.remote.import_module", return_value=mock_module):
            result_handler, result_conn = discover_remote_log_handler(
                "airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG",
                "fallback_path",
                lambda path: config,
            )

        assert result_handler is handler
        assert result_conn == "aws_default"

    def test_uses_fallback_when_no_logging_class_path(self):
        handler = DummyRemoteLogIO()
        config = {"version": 1}

        mock_module = mock.MagicMock()
        mock_module.REMOTE_TASK_LOG = handler
        mock_module.DEFAULT_REMOTE_CONN_ID = None

        with mock.patch("airflow_shared.logging.remote.import_module", return_value=mock_module):
            result_handler, _ = discover_remote_log_handler(
                "",
                "fallback_path",
                lambda path: config,
            )

        assert result_handler is handler

    def test_returns_none_when_no_remote_task_log(self):
        config = {"version": 1}

        with mock.patch(
            "airflow_shared.logging.remote.import_module",
            side_effect=ModuleNotFoundError(
                "airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG"
            ),
        ):
            result_handler, result_conn = discover_remote_log_handler(
                "airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG",
                "fallback_path",
                lambda path: config,
            )

        assert result_handler is None
        assert result_conn is None

    def test_conn_id_optional(self):
        handler = DummyRemoteLogIO()
        config = {"version": 1}

        mock_module = mock.MagicMock()
        mock_module.REMOTE_TASK_LOG = handler
        mock_module.DEFAULT_REMOTE_CONN_ID = None

        with mock.patch("airflow_shared.logging.remote.import_module", return_value=mock_module):
            result_handler, result_conn = discover_remote_log_handler(
                "airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG",
                "fallback_path",
                lambda path: config,
            )

        assert result_handler is handler
        assert result_conn is None

    def test_handles_import_error(self):
        def import_error(path):
            raise ImportError("No module named 'nonexistent'")

        result_handler, result_conn = discover_remote_log_handler(
            "nonexistent",
            "fallback_path",
            import_error,
        )

        assert result_handler is None
        assert result_conn is None

    def test_handles_module_loading_error(self):
        config = {"version": 1}

        with mock.patch(
            "airflow_shared.logging.remote.import_module", side_effect=RuntimeError("Failed to load")
        ):
            result_handler, result_conn = discover_remote_log_handler(
                "airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG",
                "fallback_path",
                lambda path: config,
            )

        assert result_handler is None
        assert result_conn is None


class TestRemoteLogIOProtocol:
    def test_dummy_implements_protocol(self):
        handler = DummyRemoteLogIO()

        assert hasattr(handler, "processors")
        assert hasattr(handler, "upload")
        assert hasattr(handler, "read")
        assert callable(handler.upload)
        assert callable(handler.read)

    def test_stream_io_protocol_runtime_check(self):
        class StreamHandler:
            @property
            def processors(self):
                return ()

            def upload(self, path, ti):
                pass

            def read(self, relative_path, ti):
                return ([], [])

            def stream(self, relative_path, ti):
                return ([], [])

        handler = StreamHandler()
        assert isinstance(handler, RemoteLogStreamIO)

    def test_non_stream_handler_not_stream_io(self):
        handler = DummyRemoteLogIO()
        assert not isinstance(handler, RemoteLogStreamIO)
