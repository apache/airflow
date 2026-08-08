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

from airflow.providers.common.compat.sdk import AirflowException

# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state


class EcsTaskFailToStart(Exception):
    """Raise when ECS tasks fail to start AFTER processing the request."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)

    def __reduce__(self):
        """Return ECSTask state and its message."""
        return EcsTaskFailToStart, (self.message)


class EcsOperatorError(Exception):
    """Raise when ECS cannot handle the request."""

    def __init__(self, failures: list, message: str):
        self.failures = failures
        self.message = message
        super().__init__(message)

    def __reduce__(self):
        """Return EcsOperator state and a tuple of failures list and message."""
        return EcsOperatorError, (self.failures, self.message)


class S3HookUriParseFailure(AirflowException):
    """When parse_s3_url fails to parse URL, this error is thrown."""


class S3HookPathTraversalError(AirflowException):
    """Raise when an S3 object key resolves outside the target local directory."""


class NeptuneGraphCreationFailedError(AirflowException):
    """Raised when a Neptune Analytics graph fails to reach the available state."""


class NeptunePrivateEndpointCreationFailedError(AirflowException):
    """Raised when a Neptune Analytics private graph endpoint fails to be created."""


class NeptunePrivateEndpointDeletionFailedError(AirflowException):
    """Raised when a Neptune Analytics private graph endpoint fails to be deleted."""


class NeptuneGraphDeletionFailedError(AirflowException):
    """Raised when a Neptune Analytics graph deletion encounters an unexpected AWS error."""


class NeptuneImportTaskCancellationFailedError(AirflowException):
    """Raised when a Neptune Analytics import task cancellation fails or returns an unexpected status."""


class NeptuneImportTaskFailedError(AirflowException):
    """Raised when a Neptune Analytics import task fails to complete successfully."""


class DataSyncTaskNotFoundError(AirflowException):
    """Raised when a DataSync task could not be identified or created for the requested locations."""


class DataSyncMultipleTasksError(AirflowException):
    """Raised when multiple DataSync tasks match and random task choice is not allowed."""


class DataSyncMultipleLocationsError(AirflowException):
    """Raised when multiple DataSync locations match and random location choice is not allowed."""


class DataSyncLocationNotFoundError(AirflowException):
    """Raised when a DataSync location could not be determined or created."""


class DataSyncTaskCreationError(AirflowException):
    """Raised when DataSync task creation did not return a task ARN."""


class DataSyncTaskExecutionFailedError(AirflowException):
    """Raised when a DataSync task execution could not be started or did not complete successfully."""
