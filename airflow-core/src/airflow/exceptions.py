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
# Note: Any AirflowException raised is expected to cause the TaskInstance
#       to be marked in an ERROR state
"""Exceptions used by Airflow."""

from __future__ import annotations

from http import HTTPStatus

from typing import TYPE_CHECKING, Any, NamedTuple

if TYPE_CHECKING:
    from airflow.models import DagRun


class AirflowException(Exception):
    """
    Base class for all Airflow's errors.

    Each custom exception should be derived from this class.
    """

    status_code = HTTPStatus.INTERNAL_SERVER_ERROR

    def serialize(self):
        cls = self.__class__
        return f"{cls.__module__}.{cls.__name__}", (str(self),), {}


class AirflowBadRequest(AirflowException):
    """Raise when the application or server cannot handle the request."""

    status_code = HTTPStatus.BAD_REQUEST


class TaskNotFound(AirflowException):
    """Raise when a Task is not available in the system."""


class AirflowNotFoundException(AirflowException):
    """Raise when the requested object/resource is not available in the system."""

    status_code = HTTPStatus.NOT_FOUND


class AirflowConfigException(AirflowException):
    """Raise when there is configuration problem."""


class InvalidStatsNameException(AirflowException):
    """Raise when name of the stats is invalid."""


class AirflowFailException(AirflowException):
    """Raise when the task should be failed without retrying."""


class AirflowOptionalProviderFeatureException(AirflowException):
    """Raise by providers when imports are missing for optional provider features."""


class AirflowInternalRuntimeError(BaseException):
    """
    Airflow Internal runtime error.

    Indicates that something really terrible happens during the Airflow execution.

    :meta private:
    """


class AirflowDagDuplicatedIdException(AirflowException):
    """Raise when a DAG's ID is already used by another DAG."""

    def __init__(self, dag_id: str, incoming: str, existing: str) -> None:
        super().__init__(dag_id, incoming, existing)
        self.dag_id = dag_id
        self.incoming = incoming
        self.existing = existing

    def __str__(self) -> str:
        return f"Ignoring DAG {self.dag_id} from {self.incoming} - also found in {self.existing}"


class AirflowClusterPolicyViolation(AirflowException):
    """Raise when there is a violation of a Cluster Policy in DAG definition."""


class AirflowClusterPolicySkipDag(AirflowException):
    """Raise when skipping dag is needed in Cluster Policy."""


class AirflowClusterPolicyError(AirflowException):
    """Raise for a Cluster Policy other than AirflowClusterPolicyViolation or AirflowClusterPolicySkipDag."""


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

    def __init__(self, dag_run: DagRun) -> None:
        super().__init__(f"A DAG Run already exists for DAG {dag_run.dag_id} with run id {dag_run.run_id}")
        self.dag_run = dag_run

    def serialize(self):
        cls = self.__class__
        # Note the DagRun object will be detached here and fails serialization, we need to create a new one
        from airflow.models import DagRun

        dag_run = DagRun(
            state=self.dag_run.state,
            dag_id=self.dag_run.dag_id,
            run_id=self.dag_run.run_id,
            run_type=self.dag_run.run_type,
        )
        dag_run.id = self.dag_run.id
        return (
            f"{cls.__module__}.{cls.__name__}",
            (),
            {"dag_run": dag_run},
        )


class SerializationError(AirflowException):
    """A problem occurred when trying to serialize something."""


class TaskInstanceNotFound(AirflowNotFoundException):
    """Raise when a task instance is not available in the system."""


class PoolNotFound(AirflowNotFoundException):
    """Raise when a Pool is not available in the system."""


class FileSyntaxError(NamedTuple):
    """Information about a single error in a file."""

    line_no: int | None
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

    def __init__(self, msg: str, file_path: str, parse_errors: list[FileSyntaxError]) -> None:
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


class AirflowUnsupportedFileTypeException(AirflowException):
    """Raise when a file type is not supported."""


class ConnectionNotUnique(AirflowException):
    """Raise when multiple values are found for the same connection ID."""


class VariableNotUnique(AirflowException):
    """Raise when multiple values are found for the same variable name."""


# The try/except handling is needed after we moved all k8s classes to cncf.kubernetes provider
# These two exceptions are used internally by Kubernetes Executor but also by PodGenerator, so we need
# to leave them here in case older version of cncf.kubernetes provider is used to run KubernetesPodOperator
# and it raises one of those exceptions. The code should be backwards compatible even if you import
# and try/except the exception using direct imports from airflow.exceptions.
# 1) if you have old provider, both provider and pod generator will throw the "airflow.exceptions" exception.
# 2) if you have new provider, both provider and pod generator will throw the
#    "airflow.providers.cncf.kubernetes" as it will be imported here from the provider.
try:
    from airflow.providers.cncf.kubernetes.exceptions import PodMutationHookException
except ImportError:

    class PodMutationHookException(AirflowException):  # type: ignore[no-redef]
        """Raised when exception happens during Pod Mutation Hook execution."""


try:
    from airflow.providers.cncf.kubernetes.exceptions import PodReconciliationError
except ImportError:

    class PodReconciliationError(AirflowException):  # type: ignore[no-redef]
        """Raised when an error is encountered while trying to merge pod configs."""


class RemovedInAirflow4Warning(DeprecationWarning):
    """Issued for usage of deprecated features that will be removed in Airflow4."""

    deprecated_since: str | None = None
    "Indicates the airflow version that started raising this deprecation warning"


class AirflowProviderDeprecationWarning(DeprecationWarning):
    """Issued for usage of deprecated features of Airflow provider."""

    deprecated_provider_since: str | None = None
    "Indicates the provider version that started raising this deprecation warning"


class DeserializingResultError(ValueError):
    """Raised when an error is encountered while a pickling library deserializes a pickle file."""

    def __str__(self):
        return (
            "Error deserializing result. Note that result deserialization "
            "is not supported across major Python versions. Cause: " + str(self.__cause__)
        )


class UnknownExecutorException(ValueError):
    """Raised when an attempt is made to load an executor which is not configured."""


_DEPRECATED_EXCEPTIONS = {
    "AirflowDagCycleException": "airflow.sdk.exceptions.AirflowDagCycleException",
    "AirflowFailException": "airflow.sdk.exceptions.AirflowFailException",
    "AirflowInactiveAssetInInletOrOutletException": "airflow.sdk.exceptions.AirflowInactiveAssetInInletOrOutletException",
    "AirflowNotFoundException": "airflow.sdk.exceptions.AirflowNotFoundException",
    "AirflowRescheduleException": "airflow.sdk.exceptions.AirflowRescheduleException",
    "AirflowSensorTimeout": "airflow.sdk.exceptions.AirflowSensorTimeout",
    "AirflowSkipException": "airflow.sdk.exceptions.AirflowSkipException",
    "AirflowTaskTerminated": "airflow.sdk.exceptions.AirflowTaskTerminated",
    "AirflowTaskTimeout": "airflow.sdk.exceptions.AirflowTaskTimeout",
    "DagRunTriggerException": "airflow.sdk.exceptions.DagRunTriggerException",
    "DownstreamTasksSkipped": "airflow.sdk.exceptions.DownstreamTasksSkipped",
    "DuplicateTaskIdFound": "airflow.sdk.exceptions.DuplicateTaskIdFound",
    "FailFastDagInvalidTriggerRule": "airflow.sdk.exceptions.FailFastDagInvalidTriggerRule",
    "ParamValidationError": "airflow.sdk.exceptions.ParamValidationError",
    "TaskAlreadyInTaskGroup": "airflow.sdk.exceptions.TaskAlreadyInTaskGroup",
    "TaskDeferred": "airflow.sdk.exceptions.TaskDeferred",
    "TaskDeferralError": "airflow.sdk.exceptions.TaskDeferralError",
    "TaskDeferralTimeout": "airflow.sdk.exceptions.TaskDeferralTimeout",
    "XComNotFound": "airflow.sdk.exceptions.XComNotFound",
}


def __getattr__(name: str):
    """Provide backward compatibility for moved exceptions."""
    if name in _DEPRECATED_EXCEPTIONS:
        import warnings
        from importlib import import_module
        from operator import attrgetter

        target_path = _DEPRECATED_EXCEPTIONS[name]
        warnings.warn(
            f"airflow.exceptions.{name} is deprecated. Use {target_path} instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        module_path, attr_name = target_path.rsplit(".", 1)
        return attrgetter(attr_name)(import_module(module_path))
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
