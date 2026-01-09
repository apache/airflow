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

import enum
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

from airflow.sdk import TriggerRule

# Re exporting AirflowConfigException from shared configuration
from airflow.sdk._shared.configuration.exceptions import AirflowConfigException as AirflowConfigException

if TYPE_CHECKING:
    from collections.abc import Collection

    from airflow.sdk.definitions.asset import AssetNameRef, AssetUniqueKey, AssetUriRef
    from airflow.sdk.execution_time.comms import ErrorResponse


class AirflowException(Exception):
    """
    Base class for all Airflow's errors.

    Each custom exception should be derived from this class.
    """

    status_code = HTTPStatus.INTERNAL_SERVER_ERROR

    def serialize(self):
        cls = self.__class__
        return f"{cls.__module__}.{cls.__name__}", (str(self),), {}


class AirflowNotFoundException(AirflowException):
    """Raise when the requested object/resource is not available in the system."""

    status_code = HTTPStatus.NOT_FOUND


class AirflowDagCycleException(AirflowException):
    """Raise when there is a cycle in Dag definition."""


class AirflowRuntimeError(Exception):
    """Generic Airflow error raised by runtime functions."""

    def __init__(self, error: ErrorResponse):
        self.error = error
        super().__init__(f"{error.error.value}: {error.detail}")


class AirflowTimetableInvalid(AirflowException):
    """Raise when a DAG has an invalid timetable."""


class ErrorType(enum.Enum):
    """Error types used in the API client."""

    CONNECTION_NOT_FOUND = "CONNECTION_NOT_FOUND"
    VARIABLE_NOT_FOUND = "VARIABLE_NOT_FOUND"
    XCOM_NOT_FOUND = "XCOM_NOT_FOUND"
    ASSET_NOT_FOUND = "ASSET_NOT_FOUND"
    DAGRUN_ALREADY_EXISTS = "DAGRUN_ALREADY_EXISTS"
    GENERIC_ERROR = "GENERIC_ERROR"
    API_SERVER_ERROR = "API_SERVER_ERROR"


class XComForMappingNotPushed(TypeError):
    """Raise when a mapped downstream's dependency fails to push XCom for task mapping."""

    def __str__(self) -> str:
        return "did not push XCom for task mapping"


class UnmappableXComTypePushed(TypeError):
    """Raise when an unmappable type is pushed as a mapped downstream's dependency."""

    def __init__(self, value: Any, *values: Any) -> None:
        super().__init__(value, *values)

    def __str__(self) -> str:
        typename = type(self.args[0]).__qualname__
        for arg in self.args[1:]:
            typename = f"{typename}[{type(arg).__qualname__}]"
        return f"unmappable return type {typename!r}"


class AirflowFailException(AirflowException):
    """Raise when the task should be failed without retrying."""


class _AirflowExecuteWithInactiveAssetExecption(AirflowFailException):
    main_message: str

    def __init__(self, inactive_asset_keys: Collection[AssetUniqueKey | AssetNameRef | AssetUriRef]) -> None:
        self.inactive_asset_keys = inactive_asset_keys

    @staticmethod
    def _render_asset_key(key: AssetUniqueKey | AssetNameRef | AssetUriRef) -> str:
        from airflow.sdk.definitions.asset import AssetNameRef, AssetUniqueKey, AssetUriRef

        if isinstance(key, AssetUniqueKey):
            return f"Asset(name={key.name!r}, uri={key.uri!r})"
        if isinstance(key, AssetNameRef):
            return f"Asset.ref(name={key.name!r})"
        if isinstance(key, AssetUriRef):
            return f"Asset.ref(uri={key.uri!r})"
        return repr(key)  # Should not happen, but let's fails more gracefully in an exception.

    def __str__(self) -> str:
        return f"{self.main_message}: {self.inactive_assets_message}"

    @property
    def inactive_assets_message(self) -> str:
        return ", ".join(self._render_asset_key(key) for key in self.inactive_asset_keys)


class AirflowInactiveAssetInInletOrOutletException(_AirflowExecuteWithInactiveAssetExecption):
    """Raise when the task is executed with inactive assets in its inlet or outlet."""

    main_message = "Task has the following inactive assets in its inlets or outlets"


class AirflowRescheduleException(AirflowException):
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


class AirflowSensorTimeout(AirflowException):
    """Raise when there is a timeout on sensor polling."""


class AirflowSkipException(AirflowException):
    """Raise when the task should be skipped."""


class AirflowTaskTerminated(BaseException):
    """Raise when the task execution is terminated."""


# Important to inherit BaseException instead of AirflowException->Exception, since this Exception is used
# to explicitly interrupt ongoing task. Code that does normal error-handling should not treat
# such interrupt as an error that can be handled normally. (Compare with KeyboardInterrupt)
class AirflowTaskTimeout(BaseException):
    """Raise when the task execution times-out."""


class TaskDeferred(BaseException):
    """
    Signal an operator moving to deferred state.

    Special exception raised to signal that the operator it was raised from
    wishes to defer until a trigger fires. Triggers can send execution back to task or end the task instance
    directly. If the trigger should end the task instance itself, ``method_name`` does not matter,
    and can be None; otherwise, provide the name of the method that should be used when
    resuming execution in the task.
    """

    def __init__(
        self,
        *,
        trigger,
        method_name: str,
        kwargs: dict[str, Any] | None = None,
        timeout=None,
    ):
        super().__init__()
        self.trigger = trigger
        self.method_name = method_name
        self.kwargs = kwargs
        self.timeout = timeout

    def serialize(self):
        cls = self.__class__
        return (
            f"{cls.__module__}.{cls.__name__}",
            (),
            {
                "trigger": self.trigger,
                "method_name": self.method_name,
                "kwargs": self.kwargs,
                "timeout": self.timeout,
            },
        )

    def __repr__(self) -> str:
        return f"<TaskDeferred trigger={self.trigger} method={self.method_name}>"


class TaskDeferralError(AirflowException):
    """Raised when a task failed during deferral for some reason."""


class TaskDeferralTimeout(AirflowException):
    """Raise when there is a timeout on the deferral."""


class DagRunTriggerException(AirflowException):
    """
    Signal by an operator to trigger a specific Dag Run of a dag.

    Special exception raised to signal that the operator it was raised from wishes to trigger
    a specific Dag Run of a dag. This is used in the ``TriggerDagRunOperator``.
    """

    def __init__(
        self,
        *,
        trigger_dag_id: str,
        dag_run_id: str,
        conf: dict | None,
        logical_date=None,
        reset_dag_run: bool,
        skip_when_already_exists: bool,
        wait_for_completion: bool,
        allowed_states: list[str],
        failed_states: list[str],
        poke_interval: int,
        deferrable: bool,
    ):
        super().__init__()
        self.trigger_dag_id = trigger_dag_id
        self.dag_run_id = dag_run_id
        self.conf = conf
        self.logical_date = logical_date
        self.reset_dag_run = reset_dag_run
        self.skip_when_already_exists = skip_when_already_exists
        self.wait_for_completion = wait_for_completion
        self.allowed_states = allowed_states
        self.failed_states = failed_states
        self.poke_interval = poke_interval
        self.deferrable = deferrable


class DownstreamTasksSkipped(AirflowException):
    """
    Signal by an operator to skip its downstream tasks.

    Special exception raised to signal that the operator it was raised from wishes to skip
    downstream tasks. This is used in the ShortCircuitOperator.

    :param tasks: List of task_ids to skip or a list of tuples with task_id and map_index to skip.
    """

    def __init__(self, *, tasks):
        super().__init__()
        self.tasks = tasks


class XComNotFound(AirflowException):
    """Raise when an XCom reference is being resolved against a non-existent XCom."""

    def __init__(self, dag_id: str, task_id: str, key: str) -> None:
        super().__init__()
        self.dag_id = dag_id
        self.task_id = task_id
        self.key = key

    def __str__(self) -> str:
        return f'XComArg result from {self.task_id} at {self.dag_id} with key="{self.key}" is not found!'

    def serialize(self):
        cls = self.__class__
        return (
            f"{cls.__module__}.{cls.__name__}",
            (),
            {"dag_id": self.dag_id, "task_id": self.task_id, "key": self.key},
        )


class ParamValidationError(AirflowException):
    """Raise when DAG params is invalid."""


class DuplicateTaskIdFound(AirflowException):
    """Raise when a Task with duplicate task_id is defined in the same DAG."""


class TaskAlreadyInTaskGroup(AirflowException):
    """Raise when a Task cannot be added to a TaskGroup since it already belongs to another TaskGroup."""

    def __init__(self, task_id: str, existing_group_id: str | None, new_group_id: str):
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


class TaskNotFound(AirflowException):
    """Raise when a Task is not available in the system."""


class FailFastDagInvalidTriggerRule(AirflowException):
    """Raise when a dag has 'fail_fast' enabled yet has a non-default trigger rule."""

    _allowed_rules = (TriggerRule.ALL_SUCCESS, TriggerRule.ALL_DONE_SETUP_SUCCESS)

    @classmethod
    def check(cls, *, fail_fast: bool, trigger_rule: TriggerRule):
        """
        Check that fail_fast dag tasks have allowable trigger rules.

        :meta private:
        """
        if fail_fast and trigger_rule not in cls._allowed_rules:
            raise cls()

    def __str__(self) -> str:
        return f"A 'fail_fast' dag can only have {TriggerRule.ALL_SUCCESS} trigger rule"


class RemovedInAirflow4Warning(DeprecationWarning):
    """Issued for usage of deprecated features that will be removed in Airflow4."""

    deprecated_since: str | None = None
    "Indicates the airflow version that started raising this deprecation warning"
