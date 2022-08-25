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
#
# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state
"""Exceptions used by Airflow"""
import datetime
import warnings
from http import HTTPStatus
from typing import Any, Dict, List, NamedTuple, Optional, Sized


class AirflowException(Exception):
    """
    Base class for all Airflow's errors.

    Each custom exception should be derived from this class.
    """

    status_code = HTTPStatus.INTERNAL_SERVER_ERROR


class AirflowBadRequest(AirflowException):
    """Raise when the application or server cannot handle the request."""

    status_code = HTTPStatus.BAD_REQUEST


class AirflowNotFoundException(AirflowException):
    """Raise when the requested object/resource is not available in the system."""

    status_code = HTTPStatus.NOT_FOUND


class AirflowConfigException(AirflowException):
    """Raise when there is configuration problem."""


class AirflowSensorTimeout(AirflowException):
    """Raise when there is a timeout on sensor polling."""


class AirflowRescheduleException(AirflowException):
    """
    Raise when the task should be re-scheduled at a later time.

    :param reschedule_date: The date when the task should be rescheduled
    """

    def __init__(self, reschedule_date):
        super().__init__()
        self.reschedule_date = reschedule_date


class InvalidStatsNameException(AirflowException):
    """Raise when name of the stats is invalid."""


class AirflowTaskTimeout(AirflowException):
    """Raise when the task execution times-out."""


class AirflowWebServerTimeout(AirflowException):
    """Raise when the web server times out."""


class AirflowSkipException(AirflowException):
    """Raise when the task should be skipped"""


class AirflowFailException(AirflowException):
    """Raise when the task should be failed without retrying."""


class AirflowOptionalProviderFeatureException(AirflowException):
    """Raise by providers when imports are missing for optional provider features."""


class XComNotFound(AirflowException):
    """Raise when an XCom reference is being resolved against a non-existent XCom."""

    def __init__(self, dag_id: str, task_id: str, key: str) -> None:
        super().__init__()
        self.dag_id = dag_id
        self.task_id = task_id
        self.key = key

    def __str__(self) -> str:
        return f'XComArg result from {self.task_id} at {self.dag_id} with key="{self.key}" is not found!'


class UnmappableOperator(AirflowException):
    """Raise when an operator is not implemented to be mappable."""


class XComForMappingNotPushed(AirflowException):
    """Raise when a mapped downstream's dependency fails to push XCom for task mapping."""

    def __str__(self) -> str:
        return "did not push XCom for task mapping"


class UnmappableXComTypePushed(AirflowException):
    """Raise when an unmappable type is pushed as a mapped downstream's dependency."""

    def __init__(self, value: Any, *values: Any) -> None:
        super().__init__(value, *values)

    def __str__(self) -> str:
        typename = type(self.args[0]).__qualname__
        for arg in self.args[1:]:
            typename = f"{typename}[{type(arg).__qualname__}]"
        return f"unmappable return type {typename!r}"


class UnmappableXComLengthPushed(AirflowException):
    """Raise when the pushed value is too large to map as a downstream's dependency."""

    def __init__(self, value: Sized, max_length: int) -> None:
        super().__init__(value)
        self.value = value
        self.max_length = max_length

    def __str__(self) -> str:
        return f"unmappable return value length: {len(self.value)} > {self.max_length}"


class AirflowDagCycleException(AirflowException):
    """Raise when there is a cycle in DAG definition."""


class AirflowDagDuplicatedIdException(AirflowException):
    """Raise when a DAG's ID is already used by another DAG."""

    def __init__(self, dag_id: str, incoming: str, existing: str) -> None:
        super().__init__(dag_id, incoming, existing)
        self.dag_id = dag_id
        self.incoming = incoming
        self.existing = existing

    def __str__(self) -> str:
        return f"Ignoring DAG {self.dag_id} from {self.incoming} - also found in {self.existing}"


class AirflowDagInconsistent(AirflowException):
    """Raise when a DAG has inconsistent attributes."""


class AirflowClusterPolicyViolation(AirflowException):
    """Raise when there is a violation of a Cluster Policy in DAG definition."""


class AirflowTimetableInvalid(AirflowException):
    """Raise when a DAG has an invalid timetable."""


class DagNotFound(AirflowNotFoundException):
    """Raise when a DAG is not available in the system."""


class DagCodeNotFound(AirflowNotFoundException):
    """Raise when a DAG code is not available in the system."""


class DagRunNotFound(AirflowNotFoundException):
    """Raise when a DAG Run is not available in the system."""


class DagRunAlreadyExists(AirflowBadRequest):
    """Raise when creating a DAG run for DAG which already has DAG run entry."""


class DagFileExists(AirflowBadRequest):
    """Raise when a DAG ID is still in DagBag i.e., DAG file is in DAG folder."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        warnings.warn("DagFileExists is deprecated and will be removed.", DeprecationWarning, stacklevel=2)


class DuplicateTaskIdFound(AirflowException):
    """Raise when a Task with duplicate task_id is defined in the same DAG."""


class TaskAlreadyInTaskGroup(AirflowException):
    """Raise when a Task cannot be added to a TaskGroup since it already belongs to another TaskGroup."""

    def __init__(self, task_id: str, existing_group_id: Optional[str], new_group_id: str) -> None:
        super().__init__(task_id, new_group_id)
        self.task_id = task_id
        self.existing_group_id = existing_group_id
        self.new_group_id = new_group_id

    def __str__(self) -> str:
        if self.existing_group_id is None:
            existing_group = "the DAG's root group"
        else:
            existing_group = f"group {self.existing_group_id!r}"
        return f"cannot add {self.task_id!r} to {self.new_group_id!r} (already in {existing_group})"


class SerializationError(AirflowException):
    """A problem occurred when trying to serialize a DAG."""


class ParamValidationError(AirflowException):
    """Raise when DAG params is invalid"""


class TaskNotFound(AirflowNotFoundException):
    """Raise when a Task is not available in the system."""


class TaskInstanceNotFound(AirflowNotFoundException):
    """Raise when a task instance is not available in the system."""


class PoolNotFound(AirflowNotFoundException):
    """Raise when a Pool is not available in the system."""


class NoAvailablePoolSlot(AirflowException):
    """Raise when there is not enough slots in pool."""


class DagConcurrencyLimitReached(AirflowException):
    """Raise when DAG max_active_tasks limit is reached."""


class TaskConcurrencyLimitReached(AirflowException):
    """Raise when task max_active_tasks limit is reached."""


class BackfillUnfinished(AirflowException):
    """
    Raises when not all tasks succeed in backfill.

    :param message: The human-readable description of the exception
    :param ti_status: The information about all task statuses
    """

    def __init__(self, message, ti_status):
        super().__init__(message)
        self.ti_status = ti_status


class FileSyntaxError(NamedTuple):
    """Information about a single error in a file."""

    line_no: Optional[int]
    message: str

    def __str__(self):
        return f"{self.message}. Line number: s{str(self.line_no)},"


class AirflowFileParseException(AirflowException):
    """
    Raises when connection or variable file can not be parsed.

    :param msg: The human-readable description of the exception
    :param file_path: A processed file that contains errors
    :param parse_errors: File syntax errors
    """

    def __init__(self, msg: str, file_path: str, parse_errors: List[FileSyntaxError]) -> None:
        super().__init__(msg)
        self.msg = msg
        self.file_path = file_path
        self.parse_errors = parse_errors

    def __str__(self):
        from airflow.utils.code_utils import prepare_code_snippet
        from airflow.utils.platform import is_tty

        result = f"{self.msg}\nFilename: {self.file_path}\n\n"

        for error_no, parse_error in enumerate(self.parse_errors, 1):
            result += "=" * 20 + f" Parse error {error_no:3} " + "=" * 20 + "\n"
            result += f"{parse_error.message}\n"
            if parse_error.line_no:
                result += f"Line number:  {parse_error.line_no}\n"
                if parse_error.line_no and is_tty():
                    result += "\n" + prepare_code_snippet(self.file_path, parse_error.line_no) + "\n"

        return result


class ConnectionNotUnique(AirflowException):
    """Raise when multiple values are found for the same connection ID."""


class TaskDeferred(BaseException):
    """
    Special exception raised to signal that the operator it was raised from
    wishes to defer until a trigger fires.
    """

    def __init__(
        self,
        *,
        trigger,
        method_name: str,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[datetime.timedelta] = None,
    ):
        super().__init__()
        self.trigger = trigger
        self.method_name = method_name
        self.kwargs = kwargs
        self.timeout = timeout
        # Check timeout type at runtime
        if self.timeout is not None and not hasattr(self.timeout, "total_seconds"):
            raise ValueError("Timeout value must be a timedelta")

    def __repr__(self) -> str:
        return f"<TaskDeferred trigger={self.trigger} method={self.method_name}>"


class TaskDeferralError(AirflowException):
    """Raised when a task failed during deferral for some reason."""


class PodReconciliationError(AirflowException):
    """Raised when an error is encountered while trying to merge pod configs."""


class RemovedInAirflow3Warning(DeprecationWarning):
    """Issued for usage of deprecated features that will be removed in Airflow3."""

    deprecated_since: Optional[str] = None
    "Indicates the airflow version that started raising this deprecation warning"


class AirflowProviderDeprecationWarning(DeprecationWarning):
    """Issued for usage of deprecated features of Airflow provider."""

    deprecated_provider_since: Optional[str] = None
    "Indicates the provider version that started raising this deprecation warning"
