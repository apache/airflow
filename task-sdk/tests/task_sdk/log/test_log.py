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

import json
import logging
import unittest.mock
from unittest import mock

import pytest
import structlog
from uuid6 import UUID

from airflow.sdk._shared.secrets_masker import SecretsMasker
from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.log import configure_logging, reset_logging


@pytest.mark.parametrize(
    "captured_logs", [(logging.INFO, "json")], indirect=True, ids=["log_level=info,formatter=json"]
)
def test_json_rendering(captured_logs):
    """
    Test that the JSON formatter renders correctly.
    """
    logger = structlog.get_logger()

    secrets_masker = SecretsMasker()

    with mock.patch("airflow.sdk._shared.secrets_masker._secrets_masker", return_value=secrets_masker):
        logger.info(
            "A test message with a Pydantic class",
            pydantic_class=TaskInstance(
                id=UUID("ffec3c8e-2898-46f8-b7d5-3cc571577368"),
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                try_number=1,
                dag_version_id=UUID("ffec3c8e-2898-46f8-b7d5-3cc571577368"),
            ),
        )
        assert captured_logs
        assert isinstance(captured_logs[0], bytes)
        assert json.loads(captured_logs[0]) == {
            "event": "A test message with a Pydantic class",
            "pydantic_class": "TaskInstance(id=UUID('ffec3c8e-2898-46f8-b7d5-3cc571577368'), task_id='test_task', dag_id='test_dag', run_id='test_run', "
            "try_number=1, dag_version_id=UUID('ffec3c8e-2898-46f8-b7d5-3cc571577368'), map_index=-1, hostname=None, context_carrier=None)",
            "timestamp": unittest.mock.ANY,
            "level": "info",
        }


@pytest.mark.parametrize(
    "captured_logs", [(logging.INFO, "json")], indirect=True, ids=["log_level=info,formatter=json"]
)
def test_jwt_token_is_redacted(captured_logs):
    """
    Tests that jwt token is redacted.
    """
    logger = structlog.get_logger()
    logger.info(
        "Executing workload",
        token="eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJ1cm46YWlyZmxvdy5hcGFjaGUub3JnOnRhc2siLCJuYmYiOjE3NDM0OTQ1NjgsImV4cCI6MTc0MzQ5NTE2OCwiaWF0IjoxNzQzNDk0NTY4LCJzdWIiOiIwMTk1ZjA1Zi1kNjRhLTc2NjMtOWQ2Yy1lYzYwYTM0MmQ5NTYifQ.df0ZNUbXwnoed2O1bjXQkPV8Df1mmMUu1b_PJrQuHoft9fhPRQELVDp-s3PtL6QYSSrF_81FzsQ7YHAu7bk-1g",
        pydantic_class=TaskInstance(
            id=UUID("ffec3c8e-2898-46f8-b7d5-3cc571577368"),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            try_number=1,
            dag_version_id=UUID("ffec3c8e-2898-46f8-b7d5-3cc571577368"),
        ),
    )
    assert captured_logs
    assert isinstance(captured_logs[0], bytes)
    assert json.loads(captured_logs[0]) == {
        "event": "Executing workload",
        "level": "info",
        "pydantic_class": "TaskInstance(id=UUID('ffec3c8e-2898-46f8-b7d5-3cc571577368'), "
        "task_id='test_task', dag_id='test_dag', run_id='test_run', "
        "try_number=1, dag_version_id=UUID('ffec3c8e-2898-46f8-b7d5-3cc571577368'), map_index=-1, hostname=None, context_carrier=None)",
        "timestamp": unittest.mock.ANY,
        "token": "eyJ***",
    }


@pytest.mark.parametrize(
    "captured_logs", [(logging.INFO, "json")], indirect=True, ids=["log_level=info,formatter=json"]
)
def test_logs_are_masked(captured_logs):
    """
    Test that JSON logs are masked.
    """
    logger = structlog.get_logger()
    secrets_masker = SecretsMasker()
    secrets_masker.add_mask("password")
    with mock.patch(
        "airflow.sdk._shared.secrets_masker.redact",
        side_effect=lambda event: {
            "event": "Connection *** is ***",
            "level": "info",
            "pydantic_class": TaskInstance(
                id=UUID("ffec3c8e-2898-46f8-b7d5-3cc571577368"),
                task_id="test_task",
                dag_id="test_dag",
                run_id="test_run",
                try_number=1,
                map_index=-1,
                hostname=None,
                dag_version_id=UUID("ffec3c8e-2898-46f8-b7d5-3cc571577368"),
            ),
            "timestamp": "2025-03-25T05:13:27.073918Z",
        },
    ):
        logger.info(
            "Connection password is password123",
            pydantic_class=TaskInstance(
                id=UUID("ffec3c8e-2898-46f8-b7d5-3cc571577368"),
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                try_number=1,
                dag_version_id=UUID("ffec3c8e-2898-46f8-b7d5-3cc571577368"),
            ),
        )
    assert captured_logs
    assert isinstance(captured_logs[0], bytes)

    log_entry = json.loads(captured_logs[0])
    assert log_entry == {
        "event": "Connection *** is ***",
        "level": "info",
        "pydantic_class": "TaskInstance(id=UUID('ffec3c8e-2898-46f8-b7d5-3cc571577368'), "
        "task_id='test_task', dag_id='test_dag', run_id='test_run', "
        "try_number=1, dag_version_id=UUID('ffec3c8e-2898-46f8-b7d5-3cc571577368'), map_index=-1, hostname=None, context_carrier=None)",
        "timestamp": "2025-03-25T05:13:27.073918Z",
    }


def test_logging_processors_with_colors():
    """Test that logging_processors creates colored console renderer when colored_console_log=True."""
    from airflow.sdk.log import logging_processors

    _, named = logging_processors(enable_pretty_log=True, colored_console_log=True)
    assert "console" in named
    console_renderer = named["console"]
    assert hasattr(console_renderer, "_styles")


def test_logging_processors_without_colors():
    """Test that logging_processors creates non-colored console renderer when colored_console_log=False."""
    from airflow.sdk.log import logging_processors

    _, named = logging_processors(enable_pretty_log=True, colored_console_log=False)
    assert "console" in named
    console_renderer = named["console"]
    assert hasattr(console_renderer, "_styles")
    assert console_renderer._styles.__name__ == "_PlainStyles"


def test_logging_processors_json_format():
    """Test that logging_processors creates JSON renderer when enable_pretty_log=False."""
    from airflow.sdk.log import logging_processors

    _, named = logging_processors(enable_pretty_log=False, colored_console_log=True)
    assert "console" not in named
    assert "json" in named


def test_configure_logging_respects_colored_console_log_config():
    """Test that configure_logging respects the colored_console_log configuration."""

    mock_conf = mock.MagicMock()
    mock_conf.get.return_value = "INFO"
    mock_conf.getboolean.return_value = False  # colored_console_log = False

    with mock.patch("airflow.configuration.conf", mock_conf):
        reset_logging()
        configure_logging(enable_pretty_log=True)
        # Check that getboolean was called with colored_console_log
        calls = [call for call in mock_conf.getboolean.call_args_list if call[0][1] == "colored_console_log"]
        assert len(calls) == 1
        assert calls[0] == mock.call("logging", "colored_console_log", fallback=True)


def test_configure_logging_explicit_colored_console_log():
    """Test that configure_logging respects explicit colored_console_log parameter."""

    mock_conf = mock.MagicMock()
    mock_conf.get.return_value = "INFO"
    mock_conf.getboolean.return_value = True  # colored_console_log = True

    with mock.patch("airflow.configuration.conf", mock_conf):
        reset_logging()
        # Explicitly disable colors despite config saying True
        configure_logging(enable_pretty_log=True, colored_console_log=False)
        mock_conf.getboolean.assert_not_called()


def test_configure_logging_no_airflow_config():
    """Test that configure_logging defaults work correctly."""

    # This test can be removed or repurposed since we now always import airflow.configuration
    mock_conf = mock.MagicMock()
    mock_conf.get.return_value = "INFO"
    mock_conf.getboolean.return_value = True  # colored_console_log = True by default

    with mock.patch("airflow.configuration.conf", mock_conf):
        reset_logging()
        configure_logging(enable_pretty_log=True)
        mock_conf.getboolean.assert_called_with("logging", "colored_console_log", fallback=True)
