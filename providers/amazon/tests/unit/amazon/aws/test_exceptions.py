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


class TestEcsExceptions:
    def test_ecs_task_fail_to_start(self):
        msg = "Task failed to start due to resource constraints"
        exc = EcsTaskFailToStart(msg)
        assert exc.message == msg
        assert str(exc) == msg
        assert isinstance(exc, Exception)
        assert not isinstance(exc, AirflowException)

        # Verify pickle / unpickle via __reduce__
        pickled = pickle.dumps(exc)
        restored = pickle.loads(pickled)
        assert isinstance(restored, EcsTaskFailToStart)
        assert restored.message == msg

    def test_ecs_operator_error(self):
        failures = [{"arn": "arn:aws:ecs:123", "reason": "RESOURCE:CPU"}]
        msg = "ECS run task error"
        exc = EcsOperatorError(failures=failures, message=msg)
        assert exc.failures == failures
        assert exc.message == msg
        assert str(exc) == msg
        assert isinstance(exc, Exception)
        assert not isinstance(exc, AirflowException)

        # Verify pickle / unpickle via __reduce__
        pickled = pickle.dumps(exc)
        restored = pickle.loads(pickled)
        assert isinstance(restored, EcsOperatorError)
        assert restored.failures == failures
        assert restored.message == msg


class TestS3Exceptions:
    def test_s3_hook_uri_parse_failure(self):
        with pytest.raises(S3HookUriParseFailure, match="Invalid S3 URI"):
            raise S3HookUriParseFailure("Invalid S3 URI")

        assert issubclass(S3HookUriParseFailure, AirflowException)

    def test_s3_hook_path_traversal_error(self):
        with pytest.raises(S3HookPathTraversalError, match="Path traversal detected"):
            raise S3HookPathTraversalError("Path traversal detected")

        assert issubclass(S3HookPathTraversalError, AirflowException)


class TestNeptuneExceptions:
    @pytest.mark.parametrize(
        "exc_class",
        [
            NeptuneGraphCreationFailedError,
            NeptunePrivateEndpointCreationFailedError,
            NeptunePrivateEndpointDeletionFailedError,
            NeptuneGraphDeletionFailedError,
            NeptuneImportTaskCancellationFailedError,
            NeptuneImportTaskFailedError,
        ],
    )
    def test_neptune_exceptions(self, exc_class):
        msg = f"Test error for {exc_class.__name__}"
        with pytest.raises(exc_class, match=msg):
            raise exc_class(msg)

        assert issubclass(exc_class, AirflowException)


class TestGlueExceptions:
    def test_glue_job_run_stopped_error(self):
        with pytest.raises(GlueJobRunStoppedError, match="Glue job stopped"):
            raise GlueJobRunStoppedError("Glue job stopped")

        assert issubclass(GlueJobRunStoppedError, AirflowException)


class TestDataSyncExceptions:
    @pytest.mark.parametrize(
        "exc_class",
        [
            DataSyncTaskNotFoundError,
            DataSyncMultipleTasksError,
            DataSyncMultipleLocationsError,
            DataSyncLocationNotFoundError,
            DataSyncTaskCreationError,
            DataSyncTaskExecutionFailedError,
        ],
    )
    def test_datasync_exceptions(self, exc_class):
        msg = f"DataSync failure for {exc_class.__name__}"
        with pytest.raises(exc_class, match=msg):
            raise exc_class(msg)

        assert issubclass(exc_class, AirflowException)


class TestWaiterExceptions:
    def test_waiter_terminal_failure(self):
        last_response = {"Status": "FAILED", "Error": "ResourceNotFound"}
        msg = "Waiter entered terminal failure"
        exc = WaiterTerminalFailure(message=msg, last_response=last_response)

        assert str(exc) == msg
        assert exc.last_response == last_response
        assert isinstance(exc, AirflowException)

        with pytest.raises(WaiterTerminalFailure, match="Waiter entered terminal failure"):
            raise exc

    def test_waiter_max_attempts_error(self):
        with pytest.raises(WaiterMaxAttemptsError, match="Max attempts exceeded"):
            raise WaiterMaxAttemptsError("Max attempts exceeded")

        assert issubclass(WaiterMaxAttemptsError, AirflowException)
