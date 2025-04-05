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
