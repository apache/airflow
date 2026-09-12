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

import pickle

import pytest

from airflow.providers.amazon.aws.exceptions import (
    DataSyncLocationNotFoundError,
    DataSyncMultipleLocationsError,
    DataSyncMultipleTasksError,
    DataSyncTaskCreationError,
    DataSyncTaskExecutionFailedError,
    DataSyncTaskNotFoundError,
    EcsOperatorError,
    EcsTaskFailToStart,
    GlueJobRunStoppedError,
    NeptuneGraphCreationFailedError,
    NeptuneGraphDeletionFailedError,
    NeptuneImportTaskCancellationFailedError,
    NeptuneImportTaskFailedError,
    NeptunePrivateEndpointCreationFailedError,
    NeptunePrivateEndpointDeletionFailedError,
    S3HookPathTraversalError,
    S3HookUriParseFailure,
    WaiterMaxAttemptsError,
    WaiterTerminalFailure,
)
from airflow.providers.common.compat.sdk import AirflowException

ECS_FAILURES = [
    {
        "arn": "arn:aws:ecs:us-east-1:123456789012:container-instance/abc123",
        "reason": "RESOURCE:MEMORY",
    }
]
LAST_RESPONSE = {"Cluster": {"Status": "FAILED", "StatusReason": "Insufficient capacity"}}

AIRFLOW_EXCEPTION_SUBCLASSES = [
    S3HookUriParseFailure,
    S3HookPathTraversalError,
    NeptuneGraphCreationFailedError,
    NeptunePrivateEndpointCreationFailedError,
    NeptunePrivateEndpointDeletionFailedError,
    NeptuneGraphDeletionFailedError,
    NeptuneImportTaskCancellationFailedError,
    NeptuneImportTaskFailedError,
    GlueJobRunStoppedError,
    DataSyncTaskNotFoundError,
    DataSyncMultipleTasksError,
    DataSyncMultipleLocationsError,
    DataSyncLocationNotFoundError,
    DataSyncTaskCreationError,
    DataSyncTaskExecutionFailedError,
    WaiterMaxAttemptsError,
]


class TestEcsTaskFailToStart:
    def test_message(self):
        exc = EcsTaskFailToStart("The task failed to start due to: OutOfMemoryError")

        assert exc.message == "The task failed to start due to: OutOfMemoryError"
        assert str(exc) == "The task failed to start due to: OutOfMemoryError"

    def test_pickle_round_trip(self):
        exc = EcsTaskFailToStart("The task failed to start due to: OutOfMemoryError")

        restored = pickle.loads(pickle.dumps(exc))

        assert isinstance(restored, EcsTaskFailToStart)
        assert restored.message == exc.message
        assert str(restored) == str(exc)


class TestEcsOperatorError:
    def test_failures_and_message(self):
        exc = EcsOperatorError(ECS_FAILURES, "ECS could not run the task")

        assert exc.failures == ECS_FAILURES
        assert exc.message == "ECS could not run the task"
        assert str(exc) == "ECS could not run the task"

    def test_pickle_round_trip(self):
        exc = EcsOperatorError(ECS_FAILURES, "ECS could not run the task")

        restored = pickle.loads(pickle.dumps(exc))

        assert isinstance(restored, EcsOperatorError)
        assert restored.failures == ECS_FAILURES
        assert restored.message == "ECS could not run the task"


class TestWaiterTerminalFailure:
    def test_message_and_last_response(self):
        exc = WaiterTerminalFailure("Waiter reached a terminal failure state", LAST_RESPONSE)

        assert isinstance(exc, AirflowException)
        assert str(exc) == "Waiter reached a terminal failure state"
        assert exc.last_response == LAST_RESPONSE

    def test_pickle_round_trip(self):
        exc = WaiterTerminalFailure("Waiter reached a terminal failure state", LAST_RESPONSE)

        restored = pickle.loads(pickle.dumps(exc))

        assert isinstance(restored, WaiterTerminalFailure)
        assert str(restored) == "Waiter reached a terminal failure state"
        assert restored.last_response == LAST_RESPONSE


@pytest.mark.parametrize("exc_cls", AIRFLOW_EXCEPTION_SUBCLASSES)
def test_airflow_exception_subclasses_keep_message(exc_cls):
    exc = exc_cls("something went wrong")

    assert isinstance(exc, AirflowException)
    assert str(exc) == "something went wrong"

    restored = pickle.loads(pickle.dumps(exc))

    assert isinstance(restored, exc_cls)
    assert str(restored) == "something went wrong"
