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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.amazon.aws.triggers.sagemaker_unified_studio import (
    SageMakerNotebookJobTrigger,
)
from airflow.triggers.base import TriggerEvent


@pytest.mark.asyncio
@patch("airflow.providers.amazon.aws.triggers.sagemaker_unified_studio.SageMakerNotebookHook")
async def test_trigger_success(mock_hook):
    mock_hook.return_value.get_notebook_execution = MagicMock(
        return_value={
            "status": "COMPLETED",
            "files": ["output.ipynb"],
            "s3_path": "s3://bucket/path",
        }
    )
    trigger = SageMakerNotebookJobTrigger(
        execution_id="exec-123",
        execution_name="my-notebook",
        waiter_delay=1,
        waiter_max_attempts=3,
    )
    gen = trigger.run()
    event = await gen.asend(None)
    assert isinstance(event, TriggerEvent)
    assert event.payload["status"] == "success"
    assert event.payload["execution_id"] == "exec-123"


@pytest.mark.asyncio
@patch("airflow.providers.amazon.aws.triggers.sagemaker_unified_studio.SageMakerNotebookHook")
async def test_trigger_failure(mock_hook):
    mock_hook.return_value.get_notebook_execution = MagicMock(
        return_value={
            "status": "FAILED",
            "error_details": {"error_message": "Something broke"},
        }
    )
    trigger = SageMakerNotebookJobTrigger(
        execution_id="exec-123",
        execution_name="my-notebook",
        waiter_delay=1,
        waiter_max_attempts=3,
    )
    gen = trigger.run()
    event = await gen.asend(None)
    assert isinstance(event, TriggerEvent)
    assert event.payload["status"] == "failed"
    assert "Something broke" in event.payload["error"]


@pytest.mark.asyncio
@patch("airflow.providers.amazon.aws.triggers.sagemaker_unified_studio.SageMakerNotebookHook")
async def test_trigger_running_then_timeout(mock_hook):
    mock_hook.return_value.get_notebook_execution = MagicMock(return_value={"status": "IN_PROGRESS"})
    trigger = SageMakerNotebookJobTrigger(
        execution_id="exec-123",
        execution_name="my-notebook",
        waiter_delay=0,
        waiter_max_attempts=2,
    )
    gen = trigger.run()
    event = await gen.asend(None)
    assert isinstance(event, TriggerEvent)
    assert event.payload["status"] == "failed"
    assert "timed out" in event.payload["error"]


def test_trigger_serialize():
    trigger = SageMakerNotebookJobTrigger(
        execution_id="exec-123",
        execution_name="my-notebook",
        waiter_delay=5,
        waiter_max_attempts=10,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == (
        "airflow.providers.amazon.aws.triggers.sagemaker_unified_studio.SageMakerNotebookJobTrigger"
    )
    assert kwargs["execution_id"] == "exec-123"
    assert kwargs["execution_name"] == "my-notebook"
    assert kwargs["waiter_delay"] == 5
    assert kwargs["waiter_max_attempts"] == 10
