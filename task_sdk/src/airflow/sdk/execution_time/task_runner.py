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

import os
import sys
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from io import FileIO
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Generic, TextIO, TypeVar

import attrs
import lazy_object_proxy
import structlog
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter

from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.listeners.listener import get_listener_manager
from airflow.sdk.api.datamodels._generated import (
    AssetProfile,
    IntermediateTIState,
    TaskInstance,
    TerminalTIState,
    TIRunContext,
)
from airflow.sdk.definitions._internal.dag_parsing_context import _airflow_parsing_context_manager
from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet
from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetNameRef, AssetUriRef
from airflow.sdk.definitions.baseoperator import BaseOperator
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.param import process_params
from airflow.sdk.execution_time.comms import (
    DeferTask,
    GetXCom,
    OKResponse,
    RescheduleTask,
    RuntimeCheckOnTask,
    SetRenderedFields,
    SetXCom,
    StartupDetails,
    SucceedTask,
    TaskState,
    ToSupervisor,
    ToTask,
    XComResult,
)
from airflow.sdk.execution_time.context import (
    ConnectionAccessor,
    MacrosAccessor,
    OutletEventAccessors,
    VariableAccessor,
    get_previous_dagrun_success,
    set_current_context,
)
from airflow.utils.net import get_hostname
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    import jinja2
    from structlog.typing import FilteringBoundLogger as Logger

    from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator
    from airflow.sdk.definitions.context import Context


class TaskRunnerMarker:
    """Marker for listener hooks, to properly detect from which component they are called."""


# TODO: Move this entire class into a separate file:
#  `airflow/sdk/execution_time/task_instance.py`
#   or `airflow/sdk/execution_time/runtime_ti.py`
class RuntimeTaskInstance(TaskInstance):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: BaseOperator
    _ti_context_from_server: Annotated[TIRunContext | None, Field(repr=False)] = None
    """The Task Instance context from the API server, if any."""

    max_tries: int = 0
    """The maximum number of retries for the task."""

    start_date: datetime
    """Start date of the task instance."""

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

        dag_run_conf = None
        if (
            self._ti_context_from_server
            and self._ti_context_from_server.dag_run
            and self._ti_context_from_server.dag_run.conf
        ):
            dag_run_conf = self._ti_context_from_server.dag_run.conf

        validated_params = process_params(self.task.dag, self.task, dag_run_conf, suppress_exception=False)

        # TODO: Assess if we need to it through airflow.utils.timezone.coerce_datetime()
        context: Context = {
            # From the Task Execution interface
            "dag": self.task.dag,
            "inlets": self.task.inlets,
            "map_index_template": self.task.map_index_template,
            "outlets": self.task.outlets,
            "run_id": self.run_id,
            "task": self.task,
            "task_instance": self,
            # TODO: Ensure that ti.log_url and such are available to use in context
            #   especially after removal of `conf` from Context.
            "ti": self,
            "outlet_events": OutletEventAccessors(),
            # "inlet_events": InletEventsAccessors(task.inlets, session=session),
            "macros": MacrosAccessor(),
            "params": validated_params,
            # TODO: Make this go through Public API longer term.
            # "test_mode": task_instance.test_mode,
            # "triggering_asset_events": lazy_object_proxy.Proxy(get_triggering_events),
            "var": {
                "json": VariableAccessor(deserialize_json=True),
                "value": VariableAccessor(deserialize_json=False),
            },
            "conn": ConnectionAccessor(),
        }
        if from_server := self._ti_context_from_server:
            dag_run = from_server.dag_run

            context_from_server: Context = {
                # TODO: Assess if we need to pass these through timezone.coerce_datetime
                "dag_run": dag_run,  # type: ignore[typeddict-item]  # Removable after #46522
                "task_instance_key_str": f"{self.task.dag_id}__{self.task.task_id}__{dag_run.run_id}",
                "task_reschedule_count": self._ti_context_from_server.task_reschedule_count or 0,
                "prev_start_date_success": lazy_object_proxy.Proxy(
                    lambda: get_previous_dagrun_success(self.id).start_date
                ),
                "prev_end_date_success": lazy_object_proxy.Proxy(
                    lambda: get_previous_dagrun_success(self.id).end_date
                ),
            }
            context.update(context_from_server)

            if logical_date := dag_run.logical_date:
                ds = logical_date.strftime("%Y-%m-%d")
                ds_nodash = ds.replace("-", "")
                ts = logical_date.isoformat()
                ts_nodash = logical_date.strftime("%Y%m%dT%H%M%S")
                ts_nodash_with_tz = ts.replace("-", "").replace(":", "")
                # logical_date and data_interval either coexist or be None together
                context.update(
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
                        "data_interval_end": dag_run.data_interval_end,
                        "data_interval_start": dag_run.data_interval_start,
                        "prev_data_interval_start_success": lazy_object_proxy.Proxy(
                            lambda: get_previous_dagrun_success(self.id).data_interval_start
                        ),
                        "prev_data_interval_end_success": lazy_object_proxy.Proxy(
                            lambda: get_previous_dagrun_success(self.id).data_interval_end
                        ),
                    }
                )

            if from_server.upstream_map_indexes is not None:
                # We stash this in here for later use, but we purposefully don't want to document it's
                # existence. Should this be a private attribute on RuntimeTI instead perhaps?
                setattr(self, "_upstream_map_indexes", from_server.upstream_map_indexes)

        return context

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

        return original_task

    def xcom_pull(
        self,
        task_ids: str | Iterable[str] | None = None,
        dag_id: str | None = None,
        key: str = "return_value",  # TODO: Make this a constant (``XCOM_RETURN_KEY``)
        include_prior_dates: bool = False,  # TODO: Add support for this
        *,
        map_indexes: int | Iterable[int] | None | ArgNotSet = NOTSET,
        default: Any = None,
        run_id: str | None = None,
    ) -> Any:
        """
        Pull XComs that optionally meet certain criteria.

        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is ``'return_value'``, also
            available as constant ``XCOM_RETURN_KEY``. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass *None*.
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Pass *None* to remove the filter.
        :param dag_id: If provided, only pulls XComs from this DAG. If *None*
            (default), the DAG of the calling task is used.
        :param map_indexes: If provided, only pull XComs with matching indexes.
            If *None* (default), this is inferred from the task(s) being pulled
            (see below for details).
        :param include_prior_dates: If False, only XComs from the current
            logical_date are returned. If *True*, XComs from previous dates
            are returned as well.
        :param run_id: If provided, only pulls XComs from a DagRun w/a matching run_id.
            If *None* (default), the run_id of the calling task is used.

        When pulling one single task (``task_id`` is *None* or a str) without
        specifying ``map_indexes``, the return value is inferred from whether
        the specified task is mapped. If not, value from the one single task
        instance is returned. If the task to pull is mapped, an iterator (not a
        list) yielding XComs from mapped task instances is returned. In either
        case, ``default`` (*None* if not specified) is returned if no matching
        XComs are found.

        When pulling multiple tasks (i.e. either ``task_id`` or ``map_index`` is
        a non-str iterable), a list of matching XComs is returned. Elements in
        the list is ordered by item ordering in ``task_id`` and ``map_index``.
        """
        if dag_id is None:
            dag_id = self.dag_id
        if run_id is None:
            run_id = self.run_id

        if task_ids is None:
            # default to the current task if not provided
            task_ids = self.task_id
        elif isinstance(task_ids, str):
            task_ids = [task_ids]
        if isinstance(map_indexes, ArgNotSet):
            map_indexes = self.map_index
        elif isinstance(map_indexes, Iterable):
            # TODO: Handle multiple map_indexes or remove support
            raise NotImplementedError("Multiple map_indexes are not supported yet")

        log = structlog.get_logger(logger_name="task")

        xcoms = []
        for t in task_ids:
            SUPERVISOR_COMMS.send_request(
                log=log,
                msg=GetXCom(
                    key=key,
                    dag_id=dag_id,
                    task_id=t,
                    run_id=run_id,
                    map_index=map_indexes,
                ),
            )

            msg = SUPERVISOR_COMMS.get_message()
            if not isinstance(msg, XComResult):
                raise TypeError(f"Expected XComResult, received: {type(msg)} {msg}")

            if msg.value is not None:
                from airflow.serialization.serde import deserialize

                # TODO: Move XCom serialization & deserialization to Task SDK
                #   https://github.com/apache/airflow/issues/45231

                # The execution API server deals in json compliant types now.
                # serde's deserialize can handle deserializing primitive, collections, and complex objects too
                xcoms.append(deserialize(msg.value))  # type: ignore[type-var]
            else:
                xcoms.append(default)

        if len(xcoms) == 1:
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


def _xcom_push(ti: RuntimeTaskInstance, key: str, value: Any, mapped_length: int | None = None) -> None:
    # Private function, as we don't want to expose the ability to manually set `mapped_length` to SDK
    # consumers
    from airflow.serialization.serde import serialize

    # TODO: Move XCom serialization & deserialization to Task SDK
    #   https://github.com/apache/airflow/issues/45231

    # The execution API server now deals in json compliant objects.
    # It is responsibility of the client to handle any non native object serialization.
    # serialize does just that.
    value = serialize(value)

    log = structlog.get_logger(logger_name="task")
    SUPERVISOR_COMMS.send_request(
        log=log,
        msg=SetXCom(
            key=key,
            value=value,
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index,
            mapped_length=mapped_length,
        ),
    )


def parse(what: StartupDetails) -> RuntimeTaskInstance:
    # TODO: Task-SDK:
    # Using DagBag here is about 98% wrong, but it'll do for now

    from airflow.models.dagbag import DagBag

    bundle_info = what.bundle_info
    bundle_instance = DagBundlesManager().get_bundle(
        name=bundle_info.name,
        version=bundle_info.version,
    )
    bundle_instance.initialize()

    dag_absolute_path = os.fspath(Path(bundle_instance.path, what.dag_rel_path))
    bag = DagBag(
        dag_folder=dag_absolute_path,
        include_examples=False,
        safe_mode=False,
        load_op_links=False,
    )
    if TYPE_CHECKING:
        assert what.ti.dag_id

    dag = bag.dags[what.ti.dag_id]

    # install_loader()

    # TODO: Handle task not found
    task = dag.task_dict[what.ti.task_id]
    if not isinstance(task, (BaseOperator, MappedOperator)):
        raise TypeError(
            f"task is of the wrong type, got {type(task)}, wanted {BaseOperator} or {MappedOperator}"
        )

    return RuntimeTaskInstance.model_construct(
        **what.ti.model_dump(exclude_unset=True),
        task=task,
        _ti_context_from_server=what.ti_context,
        max_tries=what.ti_context.max_tries,
        start_date=what.start_date,
    )


SendMsgType = TypeVar("SendMsgType", bound=BaseModel)
ReceiveMsgType = TypeVar("ReceiveMsgType", bound=BaseModel)


@attrs.define()
class CommsDecoder(Generic[ReceiveMsgType, SendMsgType]):
    """Handle communication between the task in this process and the supervisor parent process."""

    input: TextIO

    request_socket: FileIO = attrs.field(init=False, default=None)

    # We could be "clever" here and set the default to this based type parameters and a custom
    # `__class_getitem__`, but that's a lot of code the one subclass we've got currently. So we'll just use a
    # "sort of wrong default"
    decoder: TypeAdapter[ReceiveMsgType] = attrs.field(factory=lambda: TypeAdapter(ToTask), repr=False)

    def get_message(self) -> ReceiveMsgType:
        """
        Get a message from the parent.

        This will block until the message has been received.
        """
        line = self.input.readline()
        try:
            msg = self.decoder.validate_json(line)
        except Exception:
            structlog.get_logger(logger_name="CommsDecoder").exception("Unable to decode message", line=line)
            raise

        if isinstance(msg, StartupDetails):
            # If we read a startup message, pull out the FDs we care about!
            if msg.requests_fd > 0:
                self.request_socket = os.fdopen(msg.requests_fd, "wb", buffering=0)
        return msg

    def send_request(self, log: Logger, msg: SendMsgType):
        encoded_msg = msg.model_dump_json().encode() + b"\n"

        log.debug("Sending request", json=encoded_msg)
        self.request_socket.write(encoded_msg)


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


def startup() -> tuple[RuntimeTaskInstance, Logger]:
    msg = SUPERVISOR_COMMS.get_message()

    get_listener_manager().hook.on_starting(component=TaskRunnerMarker())

    if isinstance(msg, StartupDetails):
        from setproctitle import setproctitle

        setproctitle(f"airflow worker -- {msg.ti.id}")

        log = structlog.get_logger(logger_name="task")
        with _airflow_parsing_context_manager(dag_id=msg.ti.dag_id, task_id=msg.ti.task_id):
            ti = parse(msg)
        log.debug("DAG file parsed", file=msg.dag_rel_path)
    else:
        raise RuntimeError(f"Unhandled startup message {type(msg)} {msg}")

    return ti, log


def _serialize_rendered_fields(task: AbstractOperator) -> dict[str, JsonValue]:
    # TODO: Port one of the following to Task SDK
    #   airflow.serialization.helpers.serialize_template_field or
    #   airflow.models.renderedtifields.get_serialized_template_fields
    from airflow.serialization.helpers import serialize_template_field

    return {field: serialize_template_field(getattr(task, field), field) for field in task.template_fields}


def _process_outlets(context: Context, outlets: list[AssetProfile]):
    added_alias_to_task_outlet = False
    task_outlets: list[AssetProfile] = []
    outlet_events: list[Any] = []
    events = context["outlet_events"]

    for obj in outlets or []:
        # Lineage can have other types of objects besides assets
        asset_type = type(obj).__name__
        if isinstance(obj, Asset):
            task_outlets.append(AssetProfile(name=obj.name, uri=obj.uri, asset_type=asset_type))
            outlet_events.append(attrs.asdict(events[obj]))  # type: ignore
        elif isinstance(obj, AssetNameRef):
            task_outlets.append(AssetProfile(name=obj.name, asset_type=asset_type))
            # Send all events, filtering can be done in API server.
            outlet_events.append(attrs.asdict(events))  # type: ignore
        elif isinstance(obj, AssetUriRef):
            task_outlets.append(AssetProfile(uri=obj.uri, asset_type=asset_type))
            # Send all events, filtering can be done in API server.
            outlet_events.append(attrs.asdict(events))  # type: ignore
        elif isinstance(obj, AssetAlias):
            if not added_alias_to_task_outlet:
                task_outlets.append(AssetProfile(asset_type=asset_type))
                added_alias_to_task_outlet = True
            for asset_alias_event in events[obj].asset_alias_events:
                outlet_events.append(attrs.asdict(asset_alias_event))

    return task_outlets, outlet_events


def _prepare(ti: RuntimeTaskInstance, log: Logger, context: Context) -> ToSupervisor | None:
    ti.hostname = get_hostname()
    ti.task = ti.task.prepare_for_execution()
    if ti.task.inlets or ti.task.outlets:
        inlets = [asset.asprofile() for asset in ti.task.inlets if isinstance(asset, Asset)]
        outlets = [asset.asprofile() for asset in ti.task.outlets if isinstance(asset, Asset)]
        SUPERVISOR_COMMS.send_request(msg=RuntimeCheckOnTask(inlets=inlets, outlets=outlets), log=log)  # type: ignore
        ok_response = SUPERVISOR_COMMS.get_message()  # type: ignore
        if not isinstance(ok_response, OKResponse) or not ok_response.ok:
            log.info("Runtime checks failed for task, marking task as failed..")
            return TaskState(
                state=TerminalTIState.FAILED,
                end_date=datetime.now(tz=timezone.utc),
            )

    jinja_env = ti.task.dag.get_template_env()
    ti.render_templates(context=context, jinja_env=jinja_env)

    if rendered_fields := _serialize_rendered_fields(ti.task):
        # so that we do not call the API unnecessarily
        SUPERVISOR_COMMS.send_request(log=log, msg=SetRenderedFields(rendered_fields=rendered_fields))

    # TODO: Call pre execute etc.
    get_listener_manager().hook.on_task_instance_running(
        previous_state=TaskInstanceState.QUEUED, task_instance=ti
    )
    # No error, carry on and execute the task
    return None


def run(
    ti: RuntimeTaskInstance, log: Logger
) -> tuple[IntermediateTIState | TerminalTIState, ToSupervisor | None, BaseException | None]:
    """Run the task in this process."""
    from airflow.exceptions import (
        AirflowException,
        AirflowFailException,
        AirflowRescheduleException,
        AirflowSensorTimeout,
        AirflowSkipException,
        AirflowTaskTerminated,
        AirflowTaskTimeout,
        TaskDeferred,
    )

    if TYPE_CHECKING:
        assert ti.task is not None
        assert isinstance(ti.task, BaseOperator)

    msg: ToSupervisor | None = None
    state: IntermediateTIState | TerminalTIState
    error: BaseException | None = None
    try:
        context = ti.get_template_context()
        with set_current_context(context):
            # This is the earliest that we can render templates -- as if it excepts for any reason we need to
            # catch it and handle it like a normal task failure
            if early_exit := _prepare(ti, log, context):
                msg = early_exit
                state = TerminalTIState.FAILED
                return state, msg, error

            result = _execute_task(context, ti)

        _push_xcom_if_needed(result, ti, log)

        task_outlets, outlet_events = _process_outlets(context, ti.task.outlets)
        msg = SucceedTask(
            end_date=datetime.now(tz=timezone.utc),
            task_outlets=task_outlets,
            outlet_events=outlet_events,
        )
        state = TerminalTIState.SUCCESS
    except TaskDeferred as defer:
        # TODO: Should we use structlog.bind_contextvars here for dag_id, task_id & run_id?
        log.info("Pausing task as DEFERRED. ", dag_id=ti.dag_id, task_id=ti.task_id, run_id=ti.run_id)
        classpath, trigger_kwargs = defer.trigger.serialize()
        next_method = defer.method_name
        defer_timeout = defer.timeout
        msg = DeferTask(
            classpath=classpath,
            trigger_kwargs=trigger_kwargs,
            next_method=next_method,
            trigger_timeout=defer_timeout,
        )
        state = IntermediateTIState.DEFERRED
    except AirflowSkipException as e:
        if e.args:
            log.info("Skipping task.", reason=e.args[0])
        msg = TaskState(
            state=TerminalTIState.SKIPPED,
            end_date=datetime.now(tz=timezone.utc),
        )
        state = TerminalTIState.SKIPPED
    except AirflowRescheduleException as reschedule:
        log.info("Rescheduling task, marking task as UP_FOR_RESCHEDULE")
        msg = RescheduleTask(
            reschedule_date=reschedule.reschedule_date, end_date=datetime.now(tz=timezone.utc)
        )
        state = IntermediateTIState.UP_FOR_RESCHEDULE
    except (AirflowFailException, AirflowSensorTimeout) as e:
        # If AirflowFailException is raised, task should not retry.
        # If a sensor in reschedule mode reaches timeout, task should not retry.
        log.exception("Task failed with exception")
        # TODO: Handle fail_stop here: https://github.com/apache/airflow/issues/44951
        # TODO: Handle addition to Log table: https://github.com/apache/airflow/issues/44952
        msg = TaskState(
            state=TerminalTIState.FAIL_WITHOUT_RETRY,
            end_date=datetime.now(tz=timezone.utc),
        )
        state = TerminalTIState.FAIL_WITHOUT_RETRY
        error = e
    except (AirflowTaskTimeout, AirflowException) as e:
        # We should allow retries if the task has defined it.
        log.exception("Task failed with exception")
        msg = TaskState(
            state=TerminalTIState.FAILED,
            end_date=datetime.now(tz=timezone.utc),
        )
        state = TerminalTIState.FAILED
        error = e
    except AirflowTaskTerminated as e:
        # External state updates are already handled with `ti_heartbeat` and will be
        # updated already be another UI API. So, these exceptions should ideally never be thrown.
        # If these are thrown, we should mark the TI state as failed.
        log.exception("Task failed with exception")
        msg = TaskState(
            state=TerminalTIState.FAIL_WITHOUT_RETRY,
            end_date=datetime.now(tz=timezone.utc),
        )
        state = TerminalTIState.FAIL_WITHOUT_RETRY
        error = e
    except SystemExit as e:
        # SystemExit needs to be retried if they are eligible.
        log.exception("Task failed with exception")
        msg = TaskState(
            state=TerminalTIState.FAILED,
            end_date=datetime.now(tz=timezone.utc),
        )
        state = TerminalTIState.FAILED
        error = e
    except BaseException as e:
        log.exception("Task failed with exception")
        msg = TaskState(state=TerminalTIState.FAILED, end_date=datetime.now(tz=timezone.utc))
        state = TerminalTIState.FAILED
        error = e
    finally:
        if msg:
            SUPERVISOR_COMMS.send_request(msg=msg, log=log)
    # Return the message to make unit tests easier too
    return state, msg, error


def _execute_task(context: Context, ti: RuntimeTaskInstance):
    """Execute Task (optionally with a Timeout) and push Xcom results."""
    from airflow.exceptions import AirflowTaskTimeout

    task = ti.task
    if task.execution_timeout:
        # TODO: handle timeout in case of deferral
        from airflow.utils.timeout import timeout

        timeout_seconds = task.execution_timeout.total_seconds()
        try:
            # It's possible we're already timed out, so fast-fail if true
            if timeout_seconds <= 0:
                raise AirflowTaskTimeout()
            # Run task in timeout wrapper
            with timeout(timeout_seconds):
                result = task.execute(context)  # type: ignore[attr-defined]
        except AirflowTaskTimeout:
            # TODO: handle on kill callback here
            raise
    else:
        result = task.execute(context)  # type: ignore[attr-defined]
    return result


def _push_xcom_if_needed(result: Any, ti: RuntimeTaskInstance, log: Logger):
    """Push XCom values when task has ``do_xcom_push`` set to ``True`` and the task returns a result."""
    if ti.task.do_xcom_push:
        xcom_value = result
    else:
        xcom_value = None

    is_mapped = next(ti.task.iter_mapped_dependants(), None) is not None or ti.task.is_mapped

    if xcom_value is None:
        if is_mapped:
            # Uhoh, a downstream mapped task depends on us to push something to map over
            from airflow.sdk.exceptions import XComForMappingNotPushed

            raise XComForMappingNotPushed()
        return

    mapped_length: int | None = None
    if is_mapped:
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

    # TODO: Use constant for XCom return key & use serialize_value from Task SDK
    _xcom_push(ti, "return_value", result, mapped_length=mapped_length)


def finalize(
    ti: RuntimeTaskInstance, state: TerminalTIState, log: Logger, error: BaseException | None = None
):
    # Pushing xcom for each operator extra links defined on the operator only.
    for oe in ti.task.operator_extra_links:
        link, xcom_key = oe.get_link(operator=ti.task, ti_key=ti.id), oe.xcom_key  # type: ignore[arg-type]
        log.debug("Setting xcom for operator extra link", link=link, xcom_key=xcom_key)
        _xcom_push(ti, key=xcom_key, value=link)

    log.debug("Running finalizers", ti=ti)
    if state in [TerminalTIState.SUCCESS]:
        get_listener_manager().hook.on_task_instance_success(
            previous_state=TaskInstanceState.RUNNING, task_instance=ti
        )
        # TODO: Run task success callbacks here
    if state in [TerminalTIState.FAILED, TerminalTIState.FAIL_WITHOUT_RETRY]:
        get_listener_manager().hook.on_task_instance_failed(
            previous_state=TaskInstanceState.RUNNING, task_instance=ti, error=error
        )
        # TODO: Run task failure callbacks here

    get_listener_manager().hook.before_stopping(component=TaskRunnerMarker())


def main():
    # TODO: add an exception here, it causes an oof of a stack trace!
    global SUPERVISOR_COMMS
    SUPERVISOR_COMMS = CommsDecoder[ToTask, ToSupervisor](input=sys.stdin)
    try:
        ti, log = startup()
        state, msg, error = run(ti, log)
        finalize(ti, state, log, error)
    except KeyboardInterrupt:
        log = structlog.get_logger(logger_name="task")
        log.exception("Ctrl-c hit")
        exit(2)
    except Exception:
        log = structlog.get_logger(logger_name="task")
        log.exception("Top level error")
        exit(1)


if __name__ == "__main__":
    main()
