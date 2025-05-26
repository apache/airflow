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

from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.execution_time.secrets_masker import SecretsMasker


@pytest.mark.parametrize(
    "captured_logs", [(logging.INFO, "json")], indirect=True, ids=["log_level=info,formatter=json"]
)
def test_json_rendering(captured_logs):
    """
    Test that the JSON formatter renders correctly.
    """
    logger = structlog.get_logger()

    secrets_masker = SecretsMasker()

    with mock.patch("airflow.sdk.execution_time.secrets_masker._secrets_masker", return_value=secrets_masker):
        logger.info(
            "A test message with a Pydantic class",
            pydantic_class=TaskInstance(
                id=UUID("ffec3c8e-2898-46f8-b7d5-3cc571577368"),
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                try_number=1,
            ),
        )
        assert captured_logs
        assert isinstance(captured_logs[0], bytes)
        assert json.loads(captured_logs[0]) == {
            "event": "A test message with a Pydantic class",
            "pydantic_class": "TaskInstance(id=UUID('ffec3c8e-2898-46f8-b7d5-3cc571577368'), task_id='test_task', dag_id='test_dag', run_id='test_run', try_number=1, map_index=-1, hostname=None, context_carrier=None)",
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

    secrets_masker = SecretsMasker()

    with mock.patch("airflow.sdk.execution_time.secrets_masker._secrets_masker", return_value=secrets_masker):
        logger.info(
            "Executing workload",
            token="eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJ1cm46YWlyZmxvdy5hcGFjaGUub3JnOnRhc2siLCJuYmYiOjE3NDM0OTQ1NjgsImV4cCI6MTc0MzQ5NTE2OCwiaWF0IjoxNzQzNDk0NTY4LCJzdWIiOiIwMTk1ZjA1Zi1kNjRhLTc2NjMtOWQ2Yy1lYzYwYTM0MmQ5NTYifQ.df0ZNUbXwnoed2O1bjXQkPV8Df1mmMUu1b_PJrQuHoft9fhPRQELVDp-s3PtL6QYSSrF_81FzsQ7YHAu7bk-1g",
            pydantic_class=TaskInstance(
                id=UUID("ffec3c8e-2898-46f8-b7d5-3cc571577368"),
                dag_id="test_dag",
                task_id="test_task",
                run_id="test_run",
                try_number=1,
            ),
        )
        assert captured_logs
        assert isinstance(captured_logs[0], bytes)
        assert json.loads(captured_logs[0]) == {
            "event": "Executing workload",
            "level": "info",
            "pydantic_class": "TaskInstance(id=UUID('ffec3c8e-2898-46f8-b7d5-3cc571577368'), "
            "task_id='test_task', dag_id='test_dag', run_id='test_run', "
            "try_number=1, map_index=-1, hostname=None, context_carrier=None)",
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
        "airflow.sdk.execution_time.secrets_masker.redact",
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
        "try_number=1, map_index=-1, hostname=None, context_carrier=None)",
        "timestamp": "2025-03-25T05:13:27.073918Z",
    }


def test_logging_processors_with_colors():
    """Test that logging_processors creates colored console renderer when enable_colors=True."""
    from airflow.sdk.log import logging_processors

    _, named = logging_processors(enable_pretty_log=True, enable_colors=True)
    assert "console" in named
    console_renderer = named["console"]
    assert hasattr(console_renderer, "_styles")


def test_logging_processors_without_colors():
    """Test that logging_processors creates non-colored console renderer when enable_colors=False."""
    from airflow.sdk.log import logging_processors

    _, named = logging_processors(enable_pretty_log=True, enable_colors=False)
    assert "console" in named
    console_renderer = named["console"]
    assert hasattr(console_renderer, "_styles")
    assert console_renderer._styles.__name__ == "_PlainStyles"


def test_logging_processors_json_format():
    """Test that logging_processors creates JSON renderer when enable_pretty_log=False."""
    from airflow.sdk.log import logging_processors

    _, named = logging_processors(enable_pretty_log=False, enable_colors=True)
    assert "console" not in named
    assert "json" in named


def test_configure_logging_respects_colored_console_log_config():
    """Test that configure_logging respects the colored_console_log configuration."""
    from airflow.sdk.log import configure_logging, reset_logging

    mock_conf = mock.MagicMock()
    mock_conf.get.return_value = "INFO"
    mock_conf.getboolean.return_value = False  # colored_console_log = False
    mock_config_module = mock.MagicMock()
    mock_config_module.conf = mock_conf
    with mock.patch.dict("sys.modules", {"airflow.configuration": mock_config_module}):
        reset_logging()
        configure_logging(enable_pretty_log=True)
        mock_conf.getboolean.assert_called_with("logging", "colored_console_log", fallback=True)


def test_configure_logging_explicit_enable_colors():
    """Test that configure_logging respects explicit enable_colors parameter."""
    from airflow.sdk.log import configure_logging, reset_logging

    mock_conf = mock.MagicMock()
    mock_conf.get.return_value = "INFO"
    mock_conf.getboolean.return_value = True  # colored_console_log = True

    with mock.patch("airflow.sdk.log.sys.modules", {"airflow.configuration": mock.MagicMock()}):
        with mock.patch("airflow.configuration.conf", mock_conf):
            reset_logging()
            # Explicitly disable colors despite config saying True
            configure_logging(enable_pretty_log=True, enable_colors=False)
            mock_conf.getboolean.assert_not_called()


def test_configure_logging_no_airflow_config():
    """Test that configure_logging works when airflow.configuration is not available."""
    from airflow.sdk.log import configure_logging, reset_logging

    with mock.patch("airflow.sdk.log.sys.modules", {}):
        reset_logging()
        configure_logging(enable_pretty_log=True)
