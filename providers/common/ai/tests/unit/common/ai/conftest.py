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

from unittest.mock import MagicMock

import pytest
from pydantic_ai.usage import RunUsage

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk._shared.secrets_masker import reset_secrets_masker
    from airflow.sdk.log import mask_secret

REGISTERED_SECRET = "db-password-91c3"


@pytest.fixture
def register_secret():
    """
    Return a function that registers a secret with Airflow's secret masker for one test.

    The test also needs ``@pytest.mark.enable_redact``, since masking is otherwise stubbed out
    in unit tests.
    """
    if not AIRFLOW_V_3_1_PLUS:
        pytest.skip("Registering a secret in a unit test needs the Task SDK masker of Airflow 3.1+")
    reset_secrets_masker()

    def register(secret: str) -> str:
        mask_secret(secret)
        return secret

    yield register
    reset_secrets_masker()


@pytest.fixture
def registered_secret(register_secret):
    """Register ``REGISTERED_SECRET`` with Airflow's secret masker for one test."""
    return register_secret(REGISTERED_SECRET)


@pytest.fixture(autouse=True)
def isolate_hook_lineage_collector(hook_lineage_collector):
    """
    Use a fresh hook lineage collector for each common.ai unit test.

    ObjectStoragePath IO records assets on the process-wide collector in compat environments.
    """
    return None


@pytest.fixture
def make_mock_run_result():
    """Factory fixture creating a mock AgentRunResult compatible with log_run_summary.

    Returns a callable that builds a MagicMock with .output, .usage, .response,
    and .all_messages() configured so that log_run_summary can read them without
    error.

    ``cost`` defaults to ``None`` and must be set explicitly -- a MagicMock
    attribute left unconfigured returns a new (truthy, ``is not None``)
    MagicMock, which would silently push every caller through
    ``log_run_summary``'s cost-logging branch.
    """

    def _make(output, *, cost=None):
        mock_result = MagicMock()
        mock_result.output = output
        mock_result.usage = MagicMock(
            spec=RunUsage,
            requests=1,
            tool_calls=0,
            input_tokens=0,
            output_tokens=0,
            total_tokens=0,
            cache_read_tokens=0,
            cache_write_tokens=0,
            cost=cost,
        )
        mock_result.response = MagicMock(model_name="test-model")
        mock_result.all_messages.return_value = []
        return mock_result

    return _make
