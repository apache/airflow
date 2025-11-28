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
"""The entrypoint for the actual task execution process."""

from __future__ import annotations

import contextlib
import contextvars
import functools
import os
import sys
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import suppress
from datetime import datetime, timezone
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal
from urllib.parse import quote

import attrs
import lazy_object_proxy
import structlog
from pydantic import AwareDatetime, ConfigDict, Field, JsonValue, TypeAdapter

from airflow.configuration import conf
from airflow.dag_processing.bundles.base import BaseDagBundle, BundleVersionLock
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.exceptions import AirflowInactiveAssetInInletOrOutletException, AirflowTaskTimeout
from airflow.listeners.listener import get_listener_manager
from airflow.sdk.api.client import get_hostname, getuser
from airflow.sdk.api.datamodels._generated import (
    AssetProfile,
    DagRun,
    TaskInstance,
    TaskInstanceState,
    TIRunContext,
)
from airflow.sdk.bases.operator import BaseOperator, ExecutorSafeguard
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.definitions._internal.dag_parsing_context import _airflow_parsing_context_manager
from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetNameRef, AssetUniqueKey, AssetUriRef
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.param import process_params
from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
from airflow.sdk.execution_time.callback_runner import create_executable_runner
from airflow.sdk.execution_time.comms import (
    AssetEventDagRunReferenceResult,
    CommsDecoder,
    DagRunStateResult,
    DeferTask,
    DRCount,
    ErrorResponse,
    GetDagRunState,
    GetDRCount,
    GetPreviousDagRun,
    GetTaskRescheduleStartDate,
    GetTaskStates,
    GetTICount,
    InactiveAssetsResult,
    PreviousDagRunResult,
    RescheduleTask,
    ResendLoggingFD,
    RetryTask,
    SentFDs,
    SetRenderedFields,
    SkipDownstreamTasks,
    StartupDetails,
    SucceedTask,
    TaskRescheduleStartDate,
    TaskState,
    TaskStatesResult,
    TICount,
    ToSupervisor,
    ToTask,
    TriggerDagRun,
    ValidateInletsAndOutlets,
)
from airflow.sdk.execution_time.context import (
    ConnectionAccessor,
    InletEventsAccessors,
    MacrosAccessor,
    OutletEventAccessors,
    TriggeringAssetEventsAccessor,
    VariableAccessor,
    context_get_outlet_events,
    context_to_airflow_vars,
    get_previous_dagrun_success,
    set_current_context,
)
from airflow.sdk.execution_time.xcom import XCom
from airflow.sdk.timezone import coerce_datetime
from airflow.stats import Stats

if TYPE_CHECKING:
    import jinja2
    from pendulum.datetime import DateTime
    from structlog.typing import FilteringBoundLogger as Logger

    from airflow.exceptions import DagRunTriggerException, TaskDeferred
    from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.types import OutletEventAccessorsProtocol


class TaskRunnerMarker:
    """Marker for listener hooks, to properly detect from which component they are called."""


# TODO: Move this entire class into a separate file:
#  `airflow/sdk/execution_time/task_instance.py`
#   or `airflow/sdk/execution_time/runtime_ti.py`
class RuntimeTaskInstance(TaskInstance):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: BaseOperator
    bundle_instance: BaseDagBundle
    _context: Context | None = None
    """The Task Instance context."""

    _ti_context_from_server: Annotated[TIRunContext | None, Field(repr=False)] = None
    """The Task Instance context from the API server, if any."""

    max_tries: int = 0
    """The maximum number of retries for the task."""

    start_date: AwareDatetime
    """Start date of the task instance."""

    end_date: AwareDatetime | None = None

    state: TaskInstanceState | None = None

    is_mapped: bool | None = None
    """True if the original task was mapped."""

    rendered_map_index: str | None = None

    def __rich_repr__(self):
        yield "id", self.id
        yield "task_id", self.task_id
        yield "dag_id", self.dag_id
        yield "run_id", self.run_id
        yield "max_tries", self.max_tries
        yield "task", type(self.task)
        yield "start_date", self.start_date

    __rich_repr__.angular = True  # type: ignore[attr-defined]

    def get_template_context(self) -> Context:
        # TODO: Move this to `airflow.sdk.execution_time.context`
        #   once we port the entire context logic from airflow/utils/context.py ?
        from airflow.plugins_manager import integrate_macros_plugins

        integrate_macros_plugins()

        dag_run_conf: dict[str, Any] | None = None
        if from_server := self._ti_context_from_server:
            dag_run_conf = from_server.dag_run.conf or dag_run_conf

        validated_params = process_params(self.task.dag, self.task, dag_run_conf, suppress_exception=False)

        # Cache the context object, which ensures that all calls to get_template_context
        # are operating on the same context object.
        self._context: Context = self._context or {
            # From the Task Execution interface
            "dag": self.task.dag,
            "inlets": self.task.inlets,
            "map_index_template": self.task.map_index_template,
            "outlets": self.task.outlets,
            "run_id": self.run_id,
            "task": self.task,
            "task_instance": self,
            "ti": self,
            "outlet_events": OutletEventAccessors(),
            "inlet_events": InletEventsAccessors(self.task.inlets),
            "macros": MacrosAccessor(),
            "params": validated_params,
            # TODO: Make this go through Public API longer term.
            # "test_mode": task_instance.test_mode,
            "var": {
                "json": VariableAccessor(deserialize_json=True),
                "value": VariableAccessor(deserialize_json=False),
            },
            "conn": ConnectionAccessor(),
        }
        if from_server:
            dag_run = from_server.dag_run
            context_from_server: Context = {
                # TODO: Assess if we need to pass these through timezone.coerce_datetime
                "dag_run": dag_run,  # type: ignore[typeddict-item]  # Removable after #46522
                "triggering_asset_events": TriggeringAssetEventsAccessor.build(
                    AssetEventDagRunReferenceResult.from_asset_event_dag_run_reference(event)
                    for event in dag_run.consumed_asset_events
                ),
                "task_instance_key_str": f"{self.task.dag_id}__{self.task.task_id}__{dag_run.run_id}",
                "task_reschedule_count": from_server.task_reschedule_count or 0,
                "prev_start_date_success": lazy_object_proxy.Proxy(
                    lambda: coerce_datetime(get_previous_dagrun_success(self.id).start_date)
                ),
                "prev_end_date_success": lazy_object_proxy.Proxy(
                    lambda: coerce_datetime(get_previous_dagrun_success(self.id).end_date)
                ),
            }
            self._context.update(context_from_server)

            if logical_date := coerce_datetime(dag_run.logical_date):
                if TYPE_CHECKING:
                    assert isinstance(logical_date, DateTime)
                ds = logical_date.strftime("%Y-%m-%d")
                ds_nodash = ds.replace("-", "")
                ts = logical_date.isoformat()
                ts_nodash = logical_date.strftime("%Y%m%dT%H%M%S")
                ts_nodash_with_tz = ts.replace("-", "").replace(":", "")
                # logical_date and data_interval either coexist or be None together
                self._context.update(
                    {
                        # keys that depend on logical_date
                        "logical_date": logical_date,
                        "ds": ds,
                        "ds_nodash": ds_nodash,
                        "task_instance_key_str": f"{self.task.dag_id}__{self.task.task_id}__{ds_nodash}",
                        "ts": ts,
                        "ts_nodash": ts_nodash,
                        "ts_nodash_with_tz": ts_nodash_with_tz,
                        # keys that depend on data_interval
                        "data_interval_end": coerce_datetime(dag_run.data_interval_end),
                        "data_interval_start": coerce_datetime(dag_run.data_interval_start),
                        "prev_data_interval_start_success": lazy_object_proxy.Proxy(
                            lambda: coerce_datetime(get_previous_dagrun_success(self.id).data_interval_start)
                        ),
                        "prev_data_interval_end_success": lazy_object_proxy.Proxy(
                            lambda: coerce_datetime(get_previous_dagrun_success(self.id).data_interval_end)
                        ),
                    }
                )

            if from_server.upstream_map_indexes is not None:
                # We stash this in here for later use, but we purposefully don't want to document it's
                # existence. Should this be a private attribute on RuntimeTI instead perhaps?
                setattr(self, "_upstream_map_indexes", from_server.upstream_map_indexes)

        return self._context

    def render_templates(
        self, context: Context | None = None, jinja_env: jinja2.Environment | None = None
    ) -> BaseOperator:
        """
        Render templates in the operator fields.

        If the task was originally mapped, this may replace ``self.task`` with
        the unmapped, fully rendered BaseOperator. The original ``self.task``
        before replacement is returned.
        """
        if not context:
            context = self.get_template_context()
        original_task = self.task

        if TYPE_CHECKING:
            assert context

        ti = context["ti"]

        if TYPE_CHECKING:
            assert original_task
            assert self.task
            assert ti.task

        # If self.task is mapped, this call replaces self.task to point to the
        # unmapped BaseOperator created by this function! This is because the
        # MappedOperator is useless for template rendering, and we need to be
        # able to access the unmapped task instead.
        self.task.render_template_fields(context, jinja_env)
        self.is_mapped = original_task.is_mapped
        return original_task

    def xcom_pull(
        self,
        task_ids: str | Iterable[str] | None = None,
        dag_id: str | None = None,
        key: str = BaseXCom.XCOM_RETURN_KEY,
        include_prior_dates: bool = False,
        *,
        map_indexes: int | Iterable[int] | None | ArgNotSet = NOTSET,
        default: Any = None,
        run_id: str | None = None,
    ) -> Any:
        """
        Pull XComs either from the API server (BaseXCom) or from the custom XCOM backend if configured.

        The pull can be filtered optionally by certain criterion.

        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is ``'return_value'``, also
            available as constant ``XCOM_RETURN_KEY``. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass *None*.
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Pass *None* to remove the filter.
        :param dag_id: If provided, only pulls XComs from this Dag. If *None*
            (default), the Dag of the calling task is used.
        :param map_indexes: If provided, only pull XComs with matching indexes.
            If *None* (default), this is inferred from the task(s) being pulled
            (see below for details).
        :param include_prior_dates: If False, only XComs from the current
            logical_date are returned. If *True*, XComs from previous dates
            are returned as well.
        :param run_id: If provided, only pulls XComs from a DagRun w/a matching run_id.
            If *None* (default), the run_id of the calling task is used.

        When pulling one single task (``task_id`` is *None* or a str) without
        specifying ``map_indexes``, the return value is a single XCom entry
        (map_indexes is set to map_index of the calling task instance).

        When pulling task is mapped the specified ``map_index`` is used, so by default
        pulling on mapped task will result in no matching XComs if the task instance
        of the method call is not mapped. Otherwise, the map_index of the calling task
        instance is used. Setting ``map_indexes`` to *None* will pull XCom as it would
        from a non mapped task.

        In either case, ``default`` (*None* if not specified) is returned if no
        matching XComs are found.

        When pulling multiple tasks (i.e. either ``task_id`` or ``map_index`` is
        a non-str iterable), a list of matching XComs is returned. Elements in
        the list is ordered by item ordering in ``task_id`` and ``map_index``.
        """
        if dag_id is None:
            dag_id = self.dag_id
        if run_id is None:
            run_id = self.run_id

        single_task_requested = isinstance(task_ids, (str, type(None)))
        single_map_index_requested = isinstance(map_indexes, (int, type(None)))

        if task_ids is None:
            # default to the current task if not provided
            task_ids = [self.task_id]
        elif isinstance(task_ids, str):
            task_ids = [task_ids]

        # If map_indexes is not specified, pull xcoms from all map indexes for each task
        if isinstance(map_indexes, ArgNotSet):
            xcoms: list[Any] = []
            for t_id in task_ids:
                values = XCom.get_all(
                    run_id=run_id,
                    key=key,
                    task_id=t_id,
                    dag_id=dag_id,
                    include_prior_dates=include_prior_dates,
                )

                if values is None:
                    xcoms.append(None)
                else:
                    xcoms.extend(values)
            # For single task pulling from unmapped task, return single value
            if single_task_requested and len(xcoms) == 1:
                return xcoms[0]
            return xcoms

        # Original logic when map_indexes is explicitly specified
        map_indexes_iterable: Iterable[int | None] = []
        if isinstance(map_indexes, int) or map_indexes is None:
            map_indexes_iterable = [map_indexes]
        elif isinstance(map_indexes, Iterable):
            map_indexes_iterable = map_indexes
        else:
            raise TypeError(
                f"Invalid type for map_indexes: expected int, iterable of ints, or None, got {type(map_indexes)}"
            )

        xcoms = []
        for t_id, m_idx in product(task_ids, map_indexes_iterable):
            value = XCom.get_one(
                run_id=run_id,
                key=key,
                task_id=t_id,
                dag_id=dag_id,
                map_index=m_idx,
                include_prior_dates=include_prior_dates,
            )
            if value is None:
                xcoms.append(default)
            else:
                xcoms.append(value)

        if single_task_requested and single_map_index_requested:
            return xcoms[0]

        return xcoms

    def xcom_push(self, key: str, value: Any):
        """
        Make an XCom available for tasks to pull.

        :param key: Key to store the value under.
        :param value: Value to store. Only be JSON-serializable values may be used.
        """
        _xcom_push(self, key, value)

    def get_relevant_upstream_map_indexes(
        self, upstream: BaseOperator, ti_count: int | None, session: Any
    ) -> int | range | None:
        # TODO: Implement this method
        return None

    def get_first_reschedule_date(self, context: Context) -> AwareDatetime | None:
        """Get the first reschedule date for the task instance if found, none otherwise."""
        if context.get("task_reschedule_count", 0) == 0:
            # If the task has not been rescheduled, there is no need to ask the supervisor
            return None

        max_tries: int = self.max_tries
        retries: int = self.task.retries or 0
        first_try_number = max_tries - retries + 1

        log = structlog.get_logger(logger_name="task")

        log.debug("Requesting first reschedule date from supervisor")

        response = SUPERVISOR_COMMS.send(
            msg=GetTaskRescheduleStartDate(ti_id=self.id, try_number=first_try_number)
        )

        if TYPE_CHECKING:
            assert isinstance(response, TaskRescheduleStartDate)

        return response.start_date

    def get_previous_dagrun(self, state: str | None = None) -> DagRun | None:
        """Return the previous Dag run before the given logical date, optionally filtered by state."""
        context = self.get_template_context()
        dag_run = context.get("dag_run")

        log = structlog.get_logger(logger_name="task")

        log.debug("Getting previous Dag run", dag_run=dag_run)

        if dag_run is None:
            return None

        if dag_run.logical_date is None:
            return None

        response = SUPERVISOR_COMMS.send(
            msg=GetPreviousDagRun(dag_id=self.dag_id, logical_date=dag_run.logical_date, state=state)
        )

        if TYPE_CHECKING:
            assert isinstance(response, PreviousDagRunResult)

        return response.dag_run

    @staticmethod
    def get_ti_count(
        dag_id: str,
        map_index: int | None = None,
        task_ids: list[str] | None = None,
        task_group_id: str | None = None,
        logical_dates: list[datetime] | None = None,
        run_ids: list[str] | None = None,
        states: list[str] | None = None,
    ) -> int:
        """Return the number of task instances matching the given criteria."""
        response = SUPERVISOR_COMMS.send(
            GetTICount(
                dag_id=dag_id,
                map_index=map_index,
                task_ids=task_ids,
                task_group_id=task_group_id,
                logical_dates=logical_dates,
                run_ids=run_ids,
                states=states,
            ),
        )

        if TYPE_CHECKING:
            assert isinstance(response, TICount)

        return response.count

    @staticmethod
    def get_task_states(
        dag_id: str,
        map_index: int | None = None,
        task_ids: list[str] | None = None,
        task_group_id: str | None = None,
        logical_dates: list[datetime] | None = None,
        run_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        """Return the task states matching the given criteria."""
        response = SUPERVISOR_COMMS.send(
            GetTaskStates(
                dag_id=dag_id,
                map_index=map_index,
                task_ids=task_ids,
                task_group_id=task_group_id,
                logical_dates=logical_dates,
                run_ids=run_ids,
            ),
        )

        if TYPE_CHECKING:
            assert isinstance(response, TaskStatesResult)

        return response.task_states

    @staticmethod
    def get_dr_count(
        dag_id: str,
        logical_dates: list[datetime] | None = None,
        run_ids: list[str] | None = None,
        states: list[str] | None = None,
    ) -> int:
        """Return the number of Dag runs matching the given criteria."""
        response = SUPERVISOR_COMMS.send(
            GetDRCount(
                dag_id=dag_id,
                logical_dates=logical_dates,
                run_ids=run_ids,
                states=states,
            ),
        )

        if TYPE_CHECKING:
            assert isinstance(response, DRCount)

        return response.count

    @staticmethod
    def get_dagrun_state(dag_id: str, run_id: str) -> str:
        """Return the state of the Dag run with the given Run ID."""
        response = SUPERVISOR_COMMS.send(msg=GetDagRunState(dag_id=dag_id, run_id=run_id))

        if TYPE_CHECKING:
            assert isinstance(response, DagRunStateResult)

        return response.state

    @property
    def log_url(self) -> str:
        run_id = quote(self.run_id)
        base_url = conf.get("api", "base_url", fallback="http://localhost:8080/")
        map_index_value = self.map_index
        map_index = (
            f"/mapped/{map_index_value}" if map_index_value is not None and map_index_value >= 0 else ""
        )
        try_number_value = self.try_number
        try_number = (
            f"?try_number={try_number_value}" if try_number_value is not None and try_number_value > 0 else ""
        )
        _log_uri = f"{base_url.rstrip('/')}/dags/{self.dag_id}/runs/{run_id}/tasks/{self.task_id}{map_index}{try_number}"
        return _log_uri

    @property
    def mark_success_url(self) -> str:
        """URL to mark TI success."""
        return self.log_url


def _xcom_push(ti: RuntimeTaskInstance, key: str, value: Any, mapped_length: int | None = None) -> None:
    """Push a XCom through XCom.set, which pushes to XCom Backend if configured."""
    # Private function, as we don't want to expose the ability to manually set `mapped_length` to SDK
    # consumers

    XCom.set(
        key=key,
        value=value,
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        map_index=ti.map_index,
        _mapped_length=mapped_length,
    )


def _xcom_push_to_db(ti: RuntimeTaskInstance, key: str, value: Any) -> None:
    """Push a XCom directly to metadata DB, bypassing custom xcom_backend."""
    XCom._set_xcom_in_db(
        key=key,
        value=value,
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        map_index=ti.map_index,
    )


def parse(what: StartupDetails, log: Logger) -> RuntimeTaskInstance:
    # TODO: Task-SDK:
    # Using DagBag here is about 98% wrong, but it'll do for now

    from airflow.models.dagbag import DagBag

    bundle_info = what.bundle_info
    bundle_instance = DagBundlesManager().get_bundle(
        name=bundle_info.name,
        version=bundle_info.version,
    )
    bundle_instance.initialize()

    # Put bundle root on sys.path if needed. This allows the dag bundle to add
    # code in util modules to be shared between files within the same bundle.
    if (bundle_root := os.fspath(bundle_instance.path)) not in sys.path:
        sys.path.append(bundle_root)

    dag_absolute_path = os.fspath(Path(bundle_instance.path, what.dag_rel_path))
    bag = DagBag(
        dag_folder=dag_absolute_path,
        include_examples=False,
        safe_mode=False,
        load_op_links=False,
    )
    if TYPE_CHECKING:
        assert what.ti.dag_id

    try:
        dag = bag.dags[what.ti.dag_id]
    except KeyError:
        log.error(
            "Dag not found during start up", dag_id=what.ti.dag_id, bundle=bundle_info, path=what.dag_rel_path
        )
        sys.exit(1)

    # install_loader()

    try:
        task = dag.task_dict[what.ti.task_id]
    except KeyError:
        log.error(
            "Task not found in Dag during start up",
            dag_id=dag.dag_id,
            task_id=what.ti.task_id,
            bundle=bundle_info,
            path=what.dag_rel_path,
        )
        sys.exit(1)

    if not isinstance(task, (BaseOperator, MappedOperator)):
        raise TypeError(
            f"task is of the wrong type, got {type(task)}, wanted {BaseOperator} or {MappedOperator}"
        )

    return RuntimeTaskInstance.model_construct(
        **what.ti.model_dump(exclude_unset=True),
        task=task,
        bundle_instance=bundle_instance,
        _ti_context_from_server=what.ti_context,
        max_tries=what.ti_context.max_tries,
        start_date=what.start_date,
        state=TaskInstanceState.RUNNING,
    )


# This global variable will be used by Connection/Variable/XCom classes, or other parts of the task's execution,
# to send requests back to the supervisor process.
#
# Why it needs to be a global:
# - Many parts of Airflow's codebase (e.g., connections, variables, and XComs) may rely on making dynamic requests
#   to the parent process during task execution.
# - These calls occur in various locations and cannot easily pass the `CommsDecoder` instance through the
#   deeply nested execution stack.
# - By defining `SUPERVISOR_COMMS` as a global, it ensures that this communication mechanism is readily
#   accessible wherever needed during task execution without modifying every layer of the call stack.
SUPERVISOR_COMMS: CommsDecoder[ToTask, ToSupervisor]


# State machine!
# 1. Start up (receive details from supervisor)
# 2. Execution (run task code, possibly send requests)
# 3. Shutdown and report status


def startup() -> tuple[RuntimeTaskInstance, Context, Logger]:
    # The parent sends us a StartupDetails message un-prompted. After this, every single message is only sent
    # in response to us sending a request.
    log = structlog.get_logger(logger_name="task")

    if os.environ.get("_AIRFLOW__REEXECUTED_PROCESS") == "1" and (
        msgjson := os.environ.get("_AIRFLOW__STARTUP_MSG")
    ):
        # Clear any Kerberos replace cache if there is one, so new process can't reuse it.
        os.environ.pop("KRB5CCNAME", None)
        # entrypoint of re-exec process

        msg: StartupDetails = TypeAdapter(StartupDetails).validate_json(msgjson)
        reinit_supervisor_comms()

        # We delay this message until _after_ we've got the logging re-configured, otherwise it will show up
        # on stdout
        log.debug("Using serialized startup message from environment", msg=msg)
    else:
        # normal entry point
        msg = SUPERVISOR_COMMS._get_response()  # type: ignore[assignment]

        if not isinstance(msg, StartupDetails):
            raise RuntimeError(f"Unhandled startup message {type(msg)} {msg}")

    # setproctitle causes issue on Mac OS: https://github.com/benoitc/gunicorn/issues/3021
    os_type = sys.platform
    if os_type == "darwin":
        log.debug("Mac OS detected, skipping setproctitle")
    else:
        from setproctitle import setproctitle

        setproctitle(f"airflow worker -- {msg.ti.id}")

    try:
        get_listener_manager().hook.on_starting(component=TaskRunnerMarker())
    except Exception:
        log.exception("error calling listener")

    with _airflow_parsing_context_manager(dag_id=msg.ti.dag_id, task_id=msg.ti.task_id):
        ti = parse(msg, log)
    log.debug("Dag file parsed", file=msg.dag_rel_path)

    run_as_user = getattr(ti.task, "run_as_user", None) or conf.get(
        "core", "default_impersonation", fallback=None
    )

    if os.environ.get("_AIRFLOW__REEXECUTED_PROCESS") != "1" and run_as_user and run_as_user != getuser():
        # enters here for re-exec process
        os.environ["_AIRFLOW__REEXECUTED_PROCESS"] = "1"
        # store startup message in environment for re-exec process
        os.environ["_AIRFLOW__STARTUP_MSG"] = msg.model_dump_json()
        os.set_inheritable(SUPERVISOR_COMMS.socket.fileno(), True)

        # Import main directly from the module instead of re-executing the file.
        # This ensures that when other parts modules import
        # airflow.sdk.execution_time.task_runner, they get the same module instance
        # with the properly initialized SUPERVISOR_COMMS global variable.
        # If we re-executed the module with `python -m`, it would load as __main__ and future
        # imports would get a fresh copy without the initialized globals.
        rexec_python_code = "from airflow.sdk.execution_time.task_runner import main; main()"
        cmd = ["sudo", "-E", "-H", "-u", run_as_user, sys.executable, "-c", rexec_python_code]
        log.info(
            "Running command",
            command=cmd,
        )
        os.execvp("sudo", cmd)

        # ideally, we should never reach here, but if we do, we should return None, None, None
        return None, None, None

    return ti, ti.get_template_context(), log


def _serialize_rendered_fields(task: AbstractOperator) -> dict[str, JsonValue]:
    # TODO: Port one of the following to Task SDK
    #   airflow.serialization.helpers.serialize_template_field or
    #   airflow.models.renderedtifields.get_serialized_template_fields
    from airflow.sdk._shared.secrets_masker import redact
    from airflow.serialization.helpers import serialize_template_field

    rendered_fields = {}
    for field in task.template_fields:
        value = getattr(task, field)
        serialized = serialize_template_field(value, field)
        # Redact secrets in the task process itself before sending to API server
        # This ensures that the secrets those are registered via mask_secret() on workers / dag processor are properly masked
        # on the UI.
        rendered_fields[field] = redact(serialized, field)

    return rendered_fields  # type: ignore[return-value] # Convince mypy that this is OK since we pass JsonValue to redact, so it will return the same


def _build_asset_profiles(lineage_objects: list) -> Iterator[AssetProfile]:
    # Lineage can have other types of objects besides assets, so we need to process them a bit.
    for obj in lineage_objects or ():
        if isinstance(obj, Asset):
            yield AssetProfile(name=obj.name, uri=obj.uri, type=Asset.__name__)
        elif isinstance(obj, AssetNameRef):
            yield AssetProfile(name=obj.name, type=AssetNameRef.__name__)
        elif isinstance(obj, AssetUriRef):
            yield AssetProfile(uri=obj.uri, type=AssetUriRef.__name__)
        elif isinstance(obj, AssetAlias):
            yield AssetProfile(name=obj.name, type=AssetAlias.__name__)


def _serialize_outlet_events(events: OutletEventAccessorsProtocol) -> Iterator[dict[str, JsonValue]]:
    if TYPE_CHECKING:
        assert isinstance(events, OutletEventAccessors)
    # We just collect everything the user recorded in the accessors.
    # Further filtering will be done in the API server.
    for key, accessor in events._dict.items():
        if isinstance(key, AssetUniqueKey):
            yield {"dest_asset_key": attrs.asdict(key), "extra": accessor.extra}
        for alias_event in accessor.asset_alias_events:
            yield attrs.asdict(alias_event)


def _prepare(ti: RuntimeTaskInstance, log: Logger, context: Context) -> ToSupervisor | None:
    ti.hostname = get_hostname()
    ti.task = ti.task.prepare_for_execution()
    # Since context is now cached, and calling `ti.get_template_context` will return the same dict, we want to
    # update the value of the task that is sent from there
    context["task"] = ti.task

    jinja_env = ti.task.dag.get_template_env()
    ti.render_templates(context=context, jinja_env=jinja_env)

    if rendered_fields := _serialize_rendered_fields(ti.task):
        # so that we do not call the API unnecessarily
        SUPERVISOR_COMMS.send(msg=SetRenderedFields(rendered_fields=rendered_fields))

    _validate_task_inlets_and_outlets(ti=ti, log=log)

    try:
        # TODO: Call pre execute etc.
        get_listener_manager().hook.on_task_instance_running(
            previous_state=TaskInstanceState.QUEUED, task_instance=ti
        )
    except Exception:
        log.exception("error calling listener")

    # No error, carry on and execute the task
    return None


def _validate_task_inlets_and_outlets(*, ti: RuntimeTaskInstance, log: Logger) -> None:
    if not ti.task.inlets and not ti.task.outlets:
        return

    inactive_assets_resp = SUPERVISOR_COMMS.send(msg=ValidateInletsAndOutlets(ti_id=ti.id))
    if TYPE_CHECKING:
        assert isinstance(inactive_assets_resp, InactiveAssetsResult)
    if inactive_assets := inactive_assets_resp.inactive_assets:
        raise AirflowInactiveAssetInInletOrOutletException(
            inactive_asset_keys=[
                AssetUniqueKey.from_profile(asset_profile) for asset_profile in inactive_assets
            ]
        )


def _defer_task(
    defer: TaskDeferred, ti: RuntimeTaskInstance, log: Logger
) -> tuple[ToSupervisor, TaskInstanceState]:
    # TODO: Should we use structlog.bind_contextvars here for dag_id, task_id & run_id?

    log.info("Pausing task as DEFERRED. ", dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id)
    classpath, trigger_kwargs = defer.trigger.serialize()

    msg = DeferTask(
        classpath=classpath,
        trigger_kwargs=trigger_kwargs,
        trigger_timeout=defer.timeout,
        next_method=defer.method_name,
        next_kwargs=defer.kwargs or {},
    )
    state = TaskInstanceState.DEFERRED

    return msg, state


def run(
    ti: RuntimeTaskInstance,
    context: Context,
    log: Logger,
) -> tuple[TaskInstanceState, ToSupervisor | None, BaseException | None]:
    """Run the task in this process."""
    import signal

    from airflow.exceptions import (
        AirflowException,
        AirflowFailException,
        AirflowRescheduleException,
        AirflowSensorTimeout,
        AirflowSkipException,
        AirflowTaskTerminated,
        DagRunTriggerException,
        DownstreamTasksSkipped,
        TaskDeferred,
    )

    if TYPE_CHECKING:
        assert ti.task is not None
        assert isinstance(ti.task, BaseOperator)

    parent_pid = os.getpid()

    def _on_term(signum, frame):
        pid = os.getpid()
        if pid != parent_pid:
            return

        ti.task.on_kill()

    signal.signal(signal.SIGTERM, _on_term)

    msg: ToSupervisor | None = None
    state: TaskInstanceState
    error: BaseException | None = None

    try:
        # First, clear the xcom data sent from server
        if ti._ti_context_from_server and (keys_to_delete := ti._ti_context_from_server.xcom_keys_to_clear):
            for x in keys_to_delete:
                log.debug("Clearing XCom with key", key=x)
                XCom.delete(
                    key=x,
                    dag_id=ti.dag_id,
                    task_id=ti.task_id,
                    run_id=ti.run_id,
                    map_index=ti.map_index,
                )

        with set_current_context(context):
            # This is the earliest that we can render templates -- as if it excepts for any reason we need to
            # catch it and handle it like a normal task failure
            if early_exit := _prepare(ti, log, context):
                msg = early_exit
                ti.state = state = TaskInstanceState.FAILED
                return state, msg, error

            try:
                result = _execute_task(context=context, ti=ti, log=log)
            except Exception:
                import jinja2

                # If the task failed, swallow rendering error so it doesn't mask the main error.
                with contextlib.suppress(jinja2.TemplateSyntaxError, jinja2.UndefinedError):
                    ti.rendered_map_index = _render_map_index(context, ti=ti, log=log)
                raise
            else:  # If the task succeeded, render normally to let rendering error bubble up.
                ti.rendered_map_index = _render_map_index(context, ti=ti, log=log)

        _push_xcom_if_needed(result, ti, log)

        msg, state = _handle_current_task_success(context, ti)
    except DownstreamTasksSkipped as skip:
        log.info("Skipping downstream tasks.")
        tasks_to_skip = skip.tasks if isinstance(skip.tasks, list) else [skip.tasks]
        SUPERVISOR_COMMS.send(msg=SkipDownstreamTasks(tasks=tasks_to_skip))
        msg, state = _handle_current_task_success(context, ti)
    except DagRunTriggerException as drte:
        msg, state = _handle_trigger_dag_run(drte, context, ti, log)
    except TaskDeferred as defer:
        msg, state = _defer_task(defer, ti, log)
    except AirflowSkipException as e:
        if e.args:
            log.info("Skipping task.", reason=e.args[0])
        msg = TaskState(
            state=TaskInstanceState.SKIPPED,
            end_date=datetime.now(tz=timezone.utc),
            rendered_map_index=ti.rendered_map_index,
        )
        state = TaskInstanceState.SKIPPED
    except AirflowRescheduleException as reschedule:
        log.info("Rescheduling task, marking task as UP_FOR_RESCHEDULE")
        msg = RescheduleTask(
            reschedule_date=reschedule.reschedule_date, end_date=datetime.now(tz=timezone.utc)
        )
        state = TaskInstanceState.UP_FOR_RESCHEDULE
    except (AirflowFailException, AirflowSensorTimeout) as e:
        # If AirflowFailException is raised, task should not retry.
        # If a sensor in reschedule mode reaches timeout, task should not retry.
        log.exception("Task failed with exception")
        ti.end_date = datetime.now(tz=timezone.utc)
        msg = TaskState(
            state=TaskInstanceState.FAILED,
            end_date=ti.end_date,
            rendered_map_index=ti.rendered_map_index,
        )
        state = TaskInstanceState.FAILED
        error = e
    except (AirflowTaskTimeout, AirflowException, AirflowRuntimeError) as e:
        # We should allow retries if the task has defined it.
        log.exception("Task failed with exception")
        msg, state = _handle_current_task_failed(ti)
        error = e
    except AirflowTaskTerminated as e:
        # External state updates are already handled with `ti_heartbeat` and will be
        # updated already be another UI API. So, these exceptions should ideally never be thrown.
        # If these are thrown, we should mark the TI state as failed.
        log.exception("Task failed with exception")
        ti.end_date = datetime.now(tz=timezone.utc)
        msg = TaskState(
            state=TaskInstanceState.FAILED,
            end_date=ti.end_date,
            rendered_map_index=ti.rendered_map_index,
        )
        state = TaskInstanceState.FAILED
        error = e
    except SystemExit as e:
        # SystemExit needs to be retried if they are eligible.
        log.error("Task exited", exit_code=e.code)
        msg, state = _handle_current_task_failed(ti)
        error = e
    except BaseException as e:
        log.exception("Task failed with exception")
        msg, state = _handle_current_task_failed(ti)
        error = e
    finally:
        if msg:
            SUPERVISOR_COMMS.send(msg=msg)

    # Return the message to make unit tests easier too
    ti.state = state
    return state, msg, error


def _handle_current_task_success(
    context: Context,
    ti: RuntimeTaskInstance,
) -> tuple[SucceedTask, TaskInstanceState]:
    end_date = datetime.now(tz=timezone.utc)
    ti.end_date = end_date

    # Record operator and task instance success metrics
    operator = ti.task.__class__.__name__
    stats_tags = {"dag_id": ti.dag_id, "task_id": ti.task_id}

    Stats.incr(f"operator_successes_{operator}", tags=stats_tags)
    # Same metric with tagging
    Stats.incr("operator_successes", tags={**stats_tags, "operator": operator})
    Stats.incr("ti_successes", tags=stats_tags)

    task_outlets = list(_build_asset_profiles(ti.task.outlets))
    outlet_events = list(_serialize_outlet_events(context["outlet_events"]))
    msg = SucceedTask(
        end_date=end_date,
        task_outlets=task_outlets,
        outlet_events=outlet_events,
        rendered_map_index=ti.rendered_map_index,
    )
    return msg, TaskInstanceState.SUCCESS


def _handle_current_task_failed(
    ti: RuntimeTaskInstance,
) -> tuple[RetryTask, TaskInstanceState] | tuple[TaskState, TaskInstanceState]:
    end_date = datetime.now(tz=timezone.utc)
    ti.end_date = end_date
    if ti._ti_context_from_server and ti._ti_context_from_server.should_retry:
        return RetryTask(end_date=end_date), TaskInstanceState.UP_FOR_RETRY
    return (
        TaskState(
            state=TaskInstanceState.FAILED, end_date=end_date, rendered_map_index=ti.rendered_map_index
        ),
        TaskInstanceState.FAILED,
    )


def _handle_trigger_dag_run(
    drte: DagRunTriggerException, context: Context, ti: RuntimeTaskInstance, log: Logger
) -> tuple[ToSupervisor, TaskInstanceState]:
    """Handle exception from TriggerDagRunOperator."""
    log.info("Triggering Dag Run.", trigger_dag_id=drte.trigger_dag_id)
    comms_msg = SUPERVISOR_COMMS.send(
        TriggerDagRun(
            dag_id=drte.trigger_dag_id,
            run_id=drte.dag_run_id,
            logical_date=drte.logical_date,
            conf=drte.conf,
            reset_dag_run=drte.reset_dag_run,
        ),
    )

    if isinstance(comms_msg, ErrorResponse) and comms_msg.error == ErrorType.DAGRUN_ALREADY_EXISTS:
        if drte.skip_when_already_exists:
            log.info(
                "Dag Run already exists, skipping task as skip_when_already_exists is set to True.",
                dag_id=drte.trigger_dag_id,
            )
            msg = TaskState(
                state=TaskInstanceState.SKIPPED,
                end_date=datetime.now(tz=timezone.utc),
                rendered_map_index=ti.rendered_map_index,
            )
            state = TaskInstanceState.SKIPPED
        else:
            log.error("Dag Run already exists, marking task as failed.", dag_id=drte.trigger_dag_id)
            msg = TaskState(
                state=TaskInstanceState.FAILED,
                end_date=datetime.now(tz=timezone.utc),
                rendered_map_index=ti.rendered_map_index,
            )
            state = TaskInstanceState.FAILED

        return msg, state

    log.info("Dag Run triggered successfully.", trigger_dag_id=drte.trigger_dag_id)

    # Store the run id from the dag run (either created or found above) to
    # be used when creating the extra link on the webserver.
    ti.xcom_push(key="trigger_run_id", value=drte.dag_run_id)

    if drte.deferrable:
        from airflow.exceptions import TaskDeferred
        from airflow.providers.standard.triggers.external_task import DagStateTrigger

        defer = TaskDeferred(
            trigger=DagStateTrigger(
                dag_id=drte.trigger_dag_id,
                states=drte.allowed_states + drte.failed_states,  # type: ignore[arg-type]
                # Don't filter by execution_dates when run_ids is provided.
                # run_id uniquely identifies a DAG run, and when reset_dag_run=True,
                # drte.logical_date might be a newly calculated value that doesn't match
                # the persisted logical_date in the database, causing the trigger to never find the run.
                execution_dates=None,
                run_ids=[drte.dag_run_id],
                poll_interval=drte.poke_interval,
            ),
            method_name="execute_complete",
        )
        return _defer_task(defer, ti, log)
    if drte.wait_for_completion:
        while True:
            log.info(
                "Waiting for dag run to complete execution in allowed state.",
                dag_id=drte.trigger_dag_id,
                run_id=drte.dag_run_id,
                allowed_state=drte.allowed_states,
            )
            time.sleep(drte.poke_interval)

            comms_msg = SUPERVISOR_COMMS.send(
                GetDagRunState(dag_id=drte.trigger_dag_id, run_id=drte.dag_run_id)
            )
            if TYPE_CHECKING:
                assert isinstance(comms_msg, DagRunStateResult)
            if comms_msg.state in drte.failed_states:
                log.error(
                    "DagRun finished with failed state.", dag_id=drte.trigger_dag_id, state=comms_msg.state
                )
                msg = TaskState(
                    state=TaskInstanceState.FAILED,
                    end_date=datetime.now(tz=timezone.utc),
                    rendered_map_index=ti.rendered_map_index,
                )
                state = TaskInstanceState.FAILED
                return msg, state
            if comms_msg.state in drte.allowed_states:
                log.info(
                    "DagRun finished with allowed state.", dag_id=drte.trigger_dag_id, state=comms_msg.state
                )
                break
            log.debug(
                "DagRun not yet in allowed or failed state.",
                dag_id=drte.trigger_dag_id,
                state=comms_msg.state,
            )

    return _handle_current_task_success(context, ti)


def _run_task_state_change_callbacks(
    task: BaseOperator,
    kind: Literal[
        "on_execute_callback",
        "on_failure_callback",
        "on_success_callback",
        "on_retry_callback",
        "on_skipped_callback",
    ],
    context: Context,
    log: Logger,
) -> None:
    callback: Callable[[Context], None]
    for i, callback in enumerate(getattr(task, kind)):
        try:
            create_executable_runner(callback, context_get_outlet_events(context), logger=log).run(context)
        except Exception:
            log.exception("Failed to run task callback", kind=kind, index=i, callback=callback)


def _get_email_subject_content(
    *,
    task_instance: RuntimeTaskInstance,
    exception: BaseException | str | None,
    log: Logger,
) -> tuple[str, str, str]:
    """
    Get the email subject content for exceptions.

    :param task_instance: the task instance
    :param exception: the exception sent in the email
    :param task:

    :meta private:
    """
    from airflow.sdk.definitions._internal.templater import SandboxedEnvironment
    from airflow.sdk.definitions.context import Context, render_template_to_string

    exception_html = str(exception).replace("\n", "<br>")

    default_subject = "Airflow alert: {{ti}}"
    # For reporting purposes, we report based on 1-indexed,
    # not 0-indexed lists (i.e. Try 1 instead of
    # Try 0 for the first attempt).
    default_html_content = (
        "Try {{try_number}} out of {{max_tries + 1}}<br>"
        "Exception:<br>{{exception_html}}<br>"
        'Log: <a href="{{ti.log_url}}">Link</a><br>'
        "Host: {{ti.hostname}}<br>"
        'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
    )

    default_html_content_err = (
        "Try {{try_number}} out of {{max_tries + 1}}<br>"
        "Exception:<br>Failed attempt to attach error logs<br>"
        'Log: <a href="{{ti.log_url}}">Link</a><br>'
        "Host: {{ti.hostname}}<br>"
        'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
    )

    additional_context: dict[str, Any] = {
        "exception": exception,
        "exception_html": exception_html,
        "try_number": task_instance.try_number,
        "max_tries": task_instance.max_tries,
    }

    # Use the Dag's get_template_env() to set force_sandboxed. Don't add
    # the flag to the function on task object -- that function can be
    # overridden, and adding a flag breaks backward compatibility.
    dag = task_instance.task.get_dag()
    if dag:
        jinja_env = dag.get_template_env(force_sandboxed=True)
    else:
        jinja_env = SandboxedEnvironment(cache_size=0)
    jinja_context = task_instance.get_template_context()
    if not jinja_context:
        jinja_context = Context()
    # Add additional fields to the context for email template rendering
    jinja_context.update(additional_context)  # type: ignore[typeddict-item]

    def render(key: str, content: str) -> str:
        if conf.has_option("email", key):
            path = conf.get_mandatory_value("email", key)
            try:
                with open(path) as f:
                    content = f.read()
            except FileNotFoundError:
                log.warning("Could not find email template file. Using defaults...", file=path)
            except OSError:
                log.exception("Error while using email template. Using defaults...", file=path)
        return render_template_to_string(jinja_env.from_string(content), jinja_context)

    subject = render("subject_template", default_subject)
    html_content = render("html_content_template", default_html_content)
    html_content_err = render("html_content_template", default_html_content_err)

    return subject, html_content, html_content_err


def _send_task_error_email(
    to: Iterable[str],
    ti: RuntimeTaskInstance,
    exception: BaseException | str | None,
    log: Logger,
) -> None:
    from airflow.utils.email import send_email

    subject, content, err = _get_email_subject_content(task_instance=ti, exception=exception, log=log)
    try:
        send_email(to, subject, content)
    except Exception:
        send_email(to, subject, err)


def _execute_task(context: Context, ti: RuntimeTaskInstance, log: Logger):
    """Execute Task (optionally with a Timeout) and push Xcom results."""
    task = ti.task
    execute = task.execute

    if ti._ti_context_from_server and (next_method := ti._ti_context_from_server.next_method):
        from airflow.serialization.serialized_objects import BaseSerialization

        kwargs = BaseSerialization.deserialize(ti._ti_context_from_server.next_kwargs or {})

        execute = functools.partial(task.resume_execution, next_method=next_method, next_kwargs=kwargs)

    ctx = contextvars.copy_context()
    # Populate the context var so ExecutorSafeguard doesn't complain
    ctx.run(ExecutorSafeguard.tracker.set, task)

    # Export context in os.environ to make it available for operators to use.
    airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
    os.environ.update(airflow_context_vars)

    outlet_events = context_get_outlet_events(context)

    if (pre_execute_hook := task._pre_execute_hook) is not None:
        create_executable_runner(pre_execute_hook, outlet_events, logger=log).run(context)
    if getattr(pre_execute_hook := task.pre_execute, "__func__", None) is not BaseOperator.pre_execute:
        create_executable_runner(pre_execute_hook, outlet_events, logger=log).run(context)

    _run_task_state_change_callbacks(task, "on_execute_callback", context, log)

    if task.execution_timeout:
        from airflow.sdk.execution_time.timeout import timeout

        # TODO: handle timeout in case of deferral
        timeout_seconds = task.execution_timeout.total_seconds()
        try:
            # It's possible we're already timed out, so fast-fail if true
            if timeout_seconds <= 0:
                raise AirflowTaskTimeout()
            # Run task in timeout wrapper
            with timeout(timeout_seconds):
                result = ctx.run(execute, context=context)
        except AirflowTaskTimeout:
            task.on_kill()
            raise
    else:
        result = ctx.run(execute, context=context)

    if (post_execute_hook := task._post_execute_hook) is not None:
        create_executable_runner(post_execute_hook, outlet_events, logger=log).run(context, result)
    if getattr(post_execute_hook := task.post_execute, "__func__", None) is not BaseOperator.post_execute:
        create_executable_runner(post_execute_hook, outlet_events, logger=log).run(context)

    return result


def _render_map_index(context: Context, ti: RuntimeTaskInstance, log: Logger) -> str | None:
    """Render named map index if the Dag author defined map_index_template at the task level."""
    if (template := context.get("map_index_template")) is None:
        return None
    jinja_env = ti.task.dag.get_template_env()
    rendered_map_index = jinja_env.from_string(template).render(context)
    log.info("Map index rendered as %s", rendered_map_index)
    return rendered_map_index


def _push_xcom_if_needed(result: Any, ti: RuntimeTaskInstance, log: Logger):
    """Push XCom values when task has ``do_xcom_push`` set to ``True`` and the task returns a result."""
    if ti.task.do_xcom_push:
        xcom_value = result
    else:
        xcom_value = None

    has_mapped_dep = next(ti.task.iter_mapped_dependants(), None) is not None
    if xcom_value is None:
        if not ti.is_mapped and has_mapped_dep:
            # Uhoh, a downstream mapped task depends on us to push something to map over
            from airflow.sdk.exceptions import XComForMappingNotPushed

            raise XComForMappingNotPushed()
        return

    mapped_length: int | None = None
    if not ti.is_mapped and has_mapped_dep:
        from airflow.sdk.definitions.mappedoperator import is_mappable_value
        from airflow.sdk.exceptions import UnmappableXComTypePushed

        if not is_mappable_value(xcom_value):
            raise UnmappableXComTypePushed(xcom_value)
        mapped_length = len(xcom_value)

    log.info("Pushing xcom", ti=ti)

    # If the task has multiple outputs, push each output as a separate XCom.
    if ti.task.multiple_outputs:
        if not isinstance(xcom_value, Mapping):
            raise TypeError(
                f"Returned output was type {type(xcom_value)} expected dictionary for multiple_outputs"
            )
        for key in xcom_value.keys():
            if not isinstance(key, str):
                raise TypeError(
                    "Returned dictionary keys must be strings when using "
                    f"multiple_outputs, found {key} ({type(key)}) instead"
                )
        for k, v in result.items():
            ti.xcom_push(k, v)

    _xcom_push(ti, BaseXCom.XCOM_RETURN_KEY, result, mapped_length=mapped_length)


def finalize(
    ti: RuntimeTaskInstance,
    state: TaskInstanceState,
    context: Context,
    log: Logger,
    error: BaseException | None = None,
):
    # Record task duration metrics for all terminal states
    if ti.start_date and ti.end_date:
        duration_ms = (ti.end_date - ti.start_date).total_seconds() * 1000
        stats_tags = {"dag_id": ti.dag_id, "task_id": ti.task_id}

        Stats.timing(f"dag.{ti.dag_id}.{ti.task_id}.duration", duration_ms)
        Stats.timing("task.duration", duration_ms, tags=stats_tags)

    task = ti.task
    # Pushing xcom for each operator extra links defined on the operator only.
    for oe in task.operator_extra_links:
        try:
            link, xcom_key = oe.get_link(operator=task, ti_key=ti), oe.xcom_key  # type: ignore[arg-type]
            log.debug("Setting xcom for operator extra link", link=link, xcom_key=xcom_key)
            _xcom_push_to_db(ti, key=xcom_key, value=link)
        except Exception:
            log.exception(
                "Failed to push an xcom for task operator extra link",
                link_name=oe.name,
                xcom_key=oe.xcom_key,
                ti=ti,
            )

    if getattr(ti.task, "overwrite_rtif_after_execution", False):
        log.debug("Overwriting Rendered template fields.")
        if ti.task.template_fields:
            SUPERVISOR_COMMS.send(SetRenderedFields(rendered_fields=_serialize_rendered_fields(ti.task)))

    log.debug("Running finalizers", ti=ti)
    if state == TaskInstanceState.SUCCESS:
        _run_task_state_change_callbacks(task, "on_success_callback", context, log)
        try:
            get_listener_manager().hook.on_task_instance_success(
                previous_state=TaskInstanceState.RUNNING, task_instance=ti
            )
        except Exception:
            log.exception("error calling listener")
    elif state == TaskInstanceState.SKIPPED:
        _run_task_state_change_callbacks(task, "on_skipped_callback", context, log)
    elif state == TaskInstanceState.UP_FOR_RETRY:
        _run_task_state_change_callbacks(task, "on_retry_callback", context, log)
        try:
            get_listener_manager().hook.on_task_instance_failed(
                previous_state=TaskInstanceState.RUNNING, task_instance=ti, error=error
            )
        except Exception:
            log.exception("error calling listener")
        if error and task.email_on_retry and task.email:
            _send_task_error_email(task.email, ti, error, log)
    elif state == TaskInstanceState.FAILED:
        _run_task_state_change_callbacks(task, "on_failure_callback", context, log)
        try:
            get_listener_manager().hook.on_task_instance_failed(
                previous_state=TaskInstanceState.RUNNING, task_instance=ti, error=error
            )
        except Exception:
            log.exception("error calling listener")
        if error and task.email_on_failure and task.email:
            _send_task_error_email(task.email, ti, error, log)

    try:
        get_listener_manager().hook.before_stopping(component=TaskRunnerMarker())
    except Exception:
        log.exception("error calling listener")


def main():
    log = structlog.get_logger(logger_name="task")

    global SUPERVISOR_COMMS
    SUPERVISOR_COMMS = CommsDecoder[ToTask, ToSupervisor](log=log)

    try:
        ti, context, log = startup()
        with BundleVersionLock(
            bundle_name=ti.bundle_instance.name,
            bundle_version=ti.bundle_instance.version,
        ):
            state, _, error = run(ti, context, log)
            context["exception"] = error
            finalize(ti, state, context, log, error)
    except KeyboardInterrupt:
        log.exception("Ctrl-c hit")
        exit(2)
    except Exception:
        log.exception("Top level error")
        exit(1)
    finally:
        # Ensure the request socket is closed on the child side in all circumstances
        # before the process fully terminates.
        if SUPERVISOR_COMMS and SUPERVISOR_COMMS.socket:
            with suppress(Exception):
                SUPERVISOR_COMMS.socket.close()


def reinit_supervisor_comms() -> None:
    """
    Re-initialize supervisor comms and logging channel in subprocess.

    This is not needed for most cases, but is used when either we re-launch the process via sudo for
    run_as_user, or from inside the python code in a virtualenv (et al.) operator to re-connect so those tasks
    can continue to access variables etc.
    """
    import socket

    if "SUPERVISOR_COMMS" not in globals():
        global SUPERVISOR_COMMS
        log = structlog.get_logger(logger_name="task")

        fd = int(os.environ.get("__AIRFLOW_SUPERVISOR_FD", "0"))

        SUPERVISOR_COMMS = CommsDecoder[ToTask, ToSupervisor](log=log, socket=socket.socket(fileno=fd))

    logs = SUPERVISOR_COMMS.send(ResendLoggingFD())
    if isinstance(logs, SentFDs):
        from airflow.sdk.log import configure_logging

        log_io = os.fdopen(logs.fds[0], "wb", buffering=0)
        configure_logging(json_output=True, output=log_io, sending_to_supervisor=True)
    else:
        print("Unable to re-configure logging after sudo, we didn't get an FD", file=sys.stderr)


if __name__ == "__main__":
    main()
