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

import pytest

from airflow.providers.standard.exceptions import (
    AirflowExternalTaskSensorException,
    DuplicateStateError,
    ExternalDagDeletedError,
    ExternalDagFailedError,
    ExternalDagNotFoundError,
    ExternalTaskFailedError,
    ExternalTaskGroupFailedError,
    ExternalTaskGroupNotFoundError,
    ExternalTaskNotFoundError,
)


def test_external_task_sensor_exception():
    """Test if AirflowExternalTaskSensorException can be raised correctly."""
    with pytest.raises(AirflowExternalTaskSensorException, match="Task execution failed"):
        raise AirflowExternalTaskSensorException("Task execution failed")


def test_external_dag_not_found_error():
    """Test if ExternalDagNotFoundError can be raised correctly."""
    with pytest.raises(ExternalDagNotFoundError, match="External DAG not found"):
        raise ExternalDagNotFoundError("External DAG not found")

    # Verify it's a subclass of AirflowExternalTaskSensorException
    with pytest.raises(AirflowExternalTaskSensorException):
        raise ExternalDagNotFoundError("External DAG not found")


def test_external_dag_deleted_error():
    """Test if ExternalDagDeletedError can be raised correctly."""
    with pytest.raises(ExternalDagDeletedError, match="External DAG was deleted"):
        raise ExternalDagDeletedError("External DAG was deleted")

    with pytest.raises(AirflowExternalTaskSensorException):
        raise ExternalDagDeletedError("External DAG was deleted")


def test_external_task_not_found_error():
    """Test if ExternalTaskNotFoundError can be raised correctly."""
    with pytest.raises(ExternalTaskNotFoundError, match="External task not found"):
        raise ExternalTaskNotFoundError("External task not found")

    with pytest.raises(AirflowExternalTaskSensorException):
        raise ExternalTaskNotFoundError("External task not found")


def test_external_task_group_not_found_error():
    """Test if ExternalTaskGroupNotFoundError can be raised correctly."""
    with pytest.raises(ExternalTaskGroupNotFoundError, match="External task group not found"):
        raise ExternalTaskGroupNotFoundError("External task group not found")

    with pytest.raises(AirflowExternalTaskSensorException):
        raise ExternalTaskGroupNotFoundError("External task group not found")


def test_external_task_failed_error():
    """Test if ExternalTaskFailedError can be raised correctly."""
    with pytest.raises(ExternalTaskFailedError, match="External task failed"):
        raise ExternalTaskFailedError("External task failed")

    with pytest.raises(AirflowExternalTaskSensorException):
        raise ExternalTaskFailedError("External task failed")


def test_external_task_group_failed_error():
    """Test if ExternalTaskGroupFailedError can be raised correctly."""
    with pytest.raises(ExternalTaskGroupFailedError, match="External task group failed"):
        raise ExternalTaskGroupFailedError("External task group failed")

    with pytest.raises(AirflowExternalTaskSensorException):
        raise ExternalTaskGroupFailedError("External task group failed")


def test_external_dag_failed_error():
    """Test if ExternalDagFailedError can be raised correctly."""
    with pytest.raises(ExternalDagFailedError, match="External DAG failed"):
        raise ExternalDagFailedError("External DAG failed")

    with pytest.raises(AirflowExternalTaskSensorException):
        raise ExternalDagFailedError("External DAG failed")


def test_duplicate_state_error():
    """Test if DuplicateStateError can be raised correctly."""
    with pytest.raises(DuplicateStateError, match="Duplicate state provided"):
        raise DuplicateStateError("Duplicate state provided")

    with pytest.raises(AirflowExternalTaskSensorException):
        raise DuplicateStateError("Duplicate state provided")
