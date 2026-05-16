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
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from airflow.models import DagRun

# Re exporting AirflowConfigException from shared configuration
from airflow._shared.configuration.exceptions import AirflowConfigException as AirflowConfigException

# Using AirflowErrorCodeMixin from shared exceptions
from airflow._shared.exceptions import AirflowErrorCodeMixin

try:
    from airflow.sdk.exceptions import (
        AirflowException,
        AirflowNotFoundException,
        AirflowOptionalProviderFeatureException as AirflowOptionalProviderFeatureException,
        AirflowRescheduleException as AirflowRescheduleException,
        AirflowTimetableInvalid as AirflowTimetableInvalid,
        NodeNotFound as NodeNotFound,
        ParamValidationError as ParamValidationError,
        TaskNotFound as TaskNotFound,
    )
except ModuleNotFoundError:
    # When _AIRFLOW__AS_LIBRARY is set, airflow.sdk may not be installed.
    # In that case, we define fallback exception classes that mirror the SDK ones.
    class AirflowException(Exception):  # type: ignore[no-redef]
        """Base exception for Airflow errors."""

    class AirflowNotFoundException(AirflowErrorCodeMixin, AirflowException):  # type: ignore[no-redef]
        """Raise when a requested object is not found."""

        user_facing_error_message = "Requested resource was not found"

        description = (
            "This error occurs when Airflow is unable to locate a requested object "
            "during resolution. The object may not exist, may have been removed, or "
            "may not be accessible through the configured sources (such as metadata "
            "database, configuration, or external providers). All available lookup "
            "mechanisms were checked but none returned a matching result."
        )

        first_steps = (
            "Verify that the requested identifier is correct and exists. "
            "Check for typos or mismatched names. Ensure the resource is defined "
            "in the expected location (e.g., Airflow metadata database, configuration, "
            "or any configured external providers). If applicable, confirm that "
            "external systems are reachable and properly configured. "
            "Review recent changes that may have removed or renamed the resource."
        )

        documentation = "https://airflow.apache.org/docs/apache-airflow/stable/"

    class AirflowTimetableInvalid(AirflowErrorCodeMixin, AirflowException):  # type: ignore[no-redef]
        """Raise when a DAG has an invalid timetable."""

        user_facing_error_message = "Invalid timetable configuration"

        description = (
            "This error occurs when Airflow is unable to use the provided timetable "
            "definition for scheduling. The timetable may be malformed, incompatible "
            "with the current context, or fail internal validation rules required for "
            "generating valid schedules."
        )

        first_steps = (
            "Verify that the timetable definition is correct and follows the expected "
            "interface or format. If using a custom timetable, ensure it implements all "
            "required methods and returns valid schedule data. Check for recent changes "
            "to Dag or Airflow version compatibility that may affect timetable behavior. "
            "Review logs for specific validation errors that indicate which part of the "
            "timetable is invalid."
        )

        documentation = (
            "https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/timetable.html"
        )

    class TaskNotFound(AirflowErrorCodeMixin, AirflowException):  # type: ignore[no-redef]
        """Raise when a Task is not available in the system."""

        user_facing_error_message = "Requested task was not found"

        description = (
            "This error occurs when Airflow is unable to locate a task with the "
            "specified identifier in the current context. The task may not exist, "
            "may have been removed or renamed, or may not be part of the active Dag "
            "or execution scope being evaluated."
        )

        first_steps = (
            "Verify that the task_id is correct and exists in the expected Dag. "
            "Check whether the task has been renamed, removed, or conditionally "
            "excluded. Ensure you are referencing the correct Dag version and that "
            "the Dag has been successfully parsed and is active. "
            "If using dynamic task mapping or generated tasks, confirm that the "
            "task instance was created as expected during Dag run expansion."
        )

        documentation = "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html"

    class NodeNotFound(TaskNotFound, KeyError):  # type: ignore[no-redef]
        """Raise when attempting to access an invalid node (task or task group) using [] notation."""

        user_facing_error_message = "Requested Node was not found"

        description = (
            "This error occurs when Airflow attempts to evaluate, render, or traverse "
            "a specific structural component (Node) inside a Directed Acyclic Graph "
            "(Dag), but cannot locate it. It typically triggers within visual graph layouts, "
            "TaskGroup rendering, or internal serialization code when an element id "
            "fails to map to the established topology."
        )

        first_steps = (
            "Verify that the targeting identifier or key for the specific node is correctly "
            "spelled. Check the visual graph layout in the Airflow UI to confirm the node is active "
            "and rendered. Ensure that any parent-child relationships or upstream and downstream "
            "dependencies are correctly linked. Inspect the Dag serialization and parsing status to "
            "confirm the structure matches the backend metadata. Confirm that the underlying workflow "
            "definitions have not dynamically filtered or omitted the node."
        )

        documentation = "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html"

        def __str__(self) -> str:
            return str(self.args[0]) if self.args else ""

    class AirflowRescheduleException(AirflowException):  # type: ignore[no-redef]
        """
        Raise when the task should be re-scheduled at a later time.

        :param reschedule_date: The date when the task should be rescheduled
        """

        def __init__(self, reschedule_date):
            super().__init__()
            self.reschedule_date = reschedule_date

        def serialize(self):
            cls = self.__class__
            return f"{cls.__module__}.{cls.__name__}", (), {"reschedule_date": self.reschedule_date}

    class AirflowOptionalProviderFeatureException(AirflowException):  # type: ignore[no-redef]
        """Raise by providers when imports are missing for optional provider features."""

    class ParamValidationError(AirflowException, ValueError):  # type: ignore[no-redef]
        """Raise when DAG params fail validation."""


class AirflowBadRequest(AirflowException):
    """Raise when the application or server cannot handle the request."""

    status_code = HTTPStatus.BAD_REQUEST


class InvalidStatsNameException(AirflowException):
    """Raise when name of the stats is invalid."""


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


class DagNotFound(AirflowNotFoundException):
    """Raise when a DAG is not available in the system."""

    user_facing_error_message = "Requested Dag was not found"

    description = (
        "This error occurs when Airflow is unable to locate a Dag with the "
        "specified identifier within the active metadata database. It typically "
        "happens when a pipeline is requested by an external trigger, API call, "
        "or UI operation, but the scheduler or webserver has not yet registered "
        "or has recently removed the pipeline configuration."
    )

    first_steps = (
        "Verify that the Dag ID is spelled correctly and exactly matches the "
        "definition in your Python source file. Confirm that the Dag file resides "
        "in the designated Dags directory or within an active Dag bundle. Check the "
        "Airflow UI main dashboard to see if the Dag is visible, active, or currently "
        "paused. Inspect the Dag processing and serialization logs to ensure no parsing "
        "exceptions are blocking the pipeline from registering. Ensure that the scheduler "
        "or dedicated Dag file processor is running and actively scanning the directory "
        "for code updates."
    )

    documentation = "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html"


class DagCodeNotFound(AirflowNotFoundException):
    """Raise when a DAG code is not available in the system."""

    user_facing_error_message = "Requested Dag code was not found"

    description = (
        "This error occurs when Airflow tries to read, render, or display the underlying "
        "Python source code for a Dag, but cannot find the matching record in the "
        "dag_code database table. It typically happens when the Webserver or UI "
        "requests the code view for a Dag ID that has been partially removed, renamed, "
        "or hasn't been successfully synchronized by the Dag processor/scheduler."
    )

    first_steps = (
        "Check the Airflow UI dashboard to verify if the Dag ID is currently active "
        "and parsed without errors. Confirm whether the underlying Python file still "
        "exists in your Dags directory and hasn't been deleted or renamed. Verify that "
        "the Dag processor or scheduler is running actively to ensure new and modified "
        "code gets serialized into the database. Refresh the Dag structure using the UI "
        "sync buttons or trigger a parsing interval to force the scheduler to regenerate "
        "the missing code record. Inspect the database state to ensure that cleanup scripts "
        "or metadata retention loops have not accidentally purged older Dag code versions."
    )

    documentation = "https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-serialization.html"


class DagRunNotFound(AirflowNotFoundException):
    """Raise when a DAG Run is not available in the system."""

    user_facing_error_message = "Requested Dag Run was not found"

    description = (
        "This error occurs when Airflow attempts to access, execute, or evaluate "
        "a specific execution instance (Dag Run) that does not exist in the metadata "
        "database. It typically triggers when targeting a specific run_id or logical "
        "date via the UI, API, or automated pipelines without an actively recorded "
        "history for that instance."
    )

    first_steps = (
        "Check the Airflow UI to confirm if the targeted Dag exists, is active, and is unpaused. "
        "Verify whether the specific execution tracking identifier (run_id or logical date) "
        "accurately aligns with a historical entry saved in the database. Ensure an actual "
        "pipeline run has been initialized (manually or via schedule) before attempting to "
        "interact with individual task scopes. Inspect the central scheduler logs or execution "
        "logs to determine if an external process, database cleanup script, or task runner "
        "lifecycle failure prematurely deleted or omitted the run record."
    )

    documentation = "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dag-run.html"


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

    user_facing_error_message = "Requested Task Instance was not found"

    description = (
        "This error occurs when Airflow searches for a specific execution slice "
        "of a task, uniquely identified by its task_id, run_id (or execution_date), "
        "and map_index but cannot locate the corresponding row in the task_instance "
        "database table. It typically happens when querying logs, changing task "
        "states, or attempting to clear an execution for a pipeline run that does "
        "not contain that specific task record."
    )

    first_steps = (
        "Verify that the targeting task_id, run_id, and map_index values are completely accurate. "
        "Confirm that the parent Dag run exists and is actively tracked in the metadata database. "
        "Check if the task was recently added, renamed, or removed from the Python Dag file, causing "
        "a discrepancy between the active code structure and old execution histories. "
        "Ensure that database maintenance operations, aggressive metadata cleanup scripts, or manual "
        "database purges did not clear out historical task state tables prematurely. "
        "Inspect the central scheduler or webserver logs to determine if serialization lag or "
        "communication errors between distributed executors temporarily broke instance tracking."
    )

    documentation = (
        "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#task-instances"
    )


class NotMapped(Exception):
    """Raise if a task is neither mapped nor has any parent mapped groups."""


class PoolNotFound(AirflowNotFoundException):
    """Raise when a Pool is not available in the system."""

    user_facing_error_message = "Requested Pool was not found"

    description = (
        "This error occurs when a task is scheduled or triggered with a specific pool "
        "assignment, but that pool does not exist in the Airflow metadata database. It "
        "typically happens when a Dag references a custom slot limitation pool that has "
        "not yet been created via the UI, CLI, or API, or has been accidentally deleted."
    )

    first_steps = (
        "Navigate to Admin -> Pools in the Airflow UI to check the list of available slots. "
        "Verify that the pool name assigned in your task definition exactly matches an existing "
        "record. Ensure that any custom initialization scripts or deployment pipelines create "
        "the pool before execution. Check if the task can temporarily fall back to the "
        "built-in 'default_pool' while resolving the issue. Inspect the database migration and "
        "setup logs to ensure metadata tables are fully initialized."
    )

    documentation = (
        "https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html"
    )


class FileSyntaxError(NamedTuple):
    """Information about a single error in a file."""

    line_no: int | None
    message: str

    def __str__(self):
        return f"{self.message}. Line number: {str(self.line_no)},"


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


class DeserializationError(Exception):
    """
    Raised when a Dag cannot be deserialized.

    This exception should be raised using exception chaining:
    `raise DeserializationError(dag_id) from original_exception`
    """

    def __init__(self, dag_id: str | None = None, message: str | None = None):
        self.dag_id = dag_id
        if message:
            # Use custom message if provided
            super().__init__(message)
        elif dag_id is None:
            super().__init__("Missing Dag ID in serialized Dag")
        else:
            super().__init__(f"An unexpected error occurred while trying to deserialize Dag '{dag_id}'")


class DagRunTypeNotAllowed(AirflowException):
    """Raised when a Dag does not allow the requested run type."""


class AirflowClearRunningTaskException(AirflowException):
    """Raise when the user attempts to clear currently running tasks."""


_DEPRECATED_EXCEPTIONS = {
    "AirflowDagCycleException",
    "AirflowFailException",
    "AirflowInactiveAssetInInletOrOutletException",
    "AirflowSensorTimeout",
    "AirflowSkipException",
    "AirflowTaskTerminated",
    "AirflowTaskTimeout",
    "DagRunTriggerException",
    "DownstreamTasksSkipped",
    "DuplicateTaskIdFound",
    "FailFastDagInvalidTriggerRule",
    "ParamValidationError",
    "TaskAlreadyInTaskGroup",
    "TaskDeferralError",
    "TaskDeferralTimeout",
    "TaskDeferred",
    "XComNotFound",
}


def __getattr__(name: str):
    """Provide backward compatibility for moved exceptions."""
    if name in _DEPRECATED_EXCEPTIONS:
        import warnings

        from airflow import DeprecatedImportWarning
        from airflow._shared.module_loading import import_string

        target_path = f"airflow.sdk.exceptions.{name}"
        warnings.warn(
            f"airflow.exceptions.{name} is deprecated and will be removed in a future version. Use {target_path} instead.",
            DeprecatedImportWarning,
            stacklevel=2,
        )
        return import_string(target_path)
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
