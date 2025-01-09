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
from typing import TYPE_CHECKING, Annotated, Any, Generic, TextIO, TypeVar

import attrs
import structlog
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter

from airflow.sdk.api.datamodels._generated import TaskInstance, TerminalTIState, TIRunContext
from airflow.sdk.definitions.baseoperator import BaseOperator
from airflow.sdk.execution_time.comms import (
    DeferTask,
    GetXCom,
    RescheduleTask,
    SetRenderedFields,
    SetXCom,
    StartupDetails,
    TaskState,
    ToSupervisor,
    ToTask,
    XComResult,
)
from airflow.sdk.execution_time.context import (
    ConnectionAccessor,
    MacrosAccessor,
    VariableAccessor,
    set_current_context,
)

if TYPE_CHECKING:
    import jinja2
    from structlog.typing import FilteringBoundLogger as Logger


# TODO: Move this entire class into a separate file:
#  `airflow/sdk/execution_time/task_instance.py`
#   or `airflow/sdk/execution_time/runtime_ti.py`
class RuntimeTaskInstance(TaskInstance):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: BaseOperator
    _ti_context_from_server: Annotated[TIRunContext | None, Field(repr=False)] = None
    """The Task Instance context from the API server, if any."""

    def get_template_context(self):
        # TODO: Move this to `airflow.sdk.execution_time.context`
        #   once we port the entire context logic from airflow/utils/context.py ?

        # TODO: Assess if we need to it through airflow.utils.timezone.coerce_datetime()
        context: dict[str, Any] = {
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
            # "outlet_events": OutletEventAccessors(),
            # "expanded_ti_count": expanded_ti_count,
            "expanded_ti_count": None,  # TODO: Implement this
            # "inlet_events": InletEventsAccessors(task.inlets, session=session),
            "macros": MacrosAccessor(),
            # "params": validated_params,
            # "prev_data_interval_start_success": get_prev_data_interval_start_success(),
            # "prev_data_interval_end_success": get_prev_data_interval_end_success(),
            # "prev_start_date_success": get_prev_start_date_success(),
            # "prev_end_date_success": get_prev_end_date_success(),
            # "test_mode": task_instance.test_mode,
            # "triggering_asset_events": lazy_object_proxy.Proxy(get_triggering_events),
            "var": {
                "json": VariableAccessor(deserialize_json=True),
                "value": VariableAccessor(deserialize_json=False),
            },
            "conn": ConnectionAccessor(),
        }
        if self._ti_context_from_server:
            dag_run = self._ti_context_from_server.dag_run

            logical_date = dag_run.logical_date
            ds = logical_date.strftime("%Y-%m-%d")
            ds_nodash = ds.replace("-", "")
            ts = logical_date.isoformat()
            ts_nodash = logical_date.strftime("%Y%m%dT%H%M%S")
            ts_nodash_with_tz = ts.replace("-", "").replace(":", "")

            context_from_server = {
                # TODO: Assess if we need to pass these through timezone.coerce_datetime
                "dag_run": dag_run,
                "data_interval_end": dag_run.data_interval_end,
                "data_interval_start": dag_run.data_interval_start,
                "logical_date": logical_date,
                "ds": ds,
                "ds_nodash": ds_nodash,
                "task_instance_key_str": f"{self.task.dag_id}__{self.task.task_id}__{ds_nodash}",
                "ts": ts,
                "ts_nodash": ts_nodash,
                "ts_nodash_with_tz": ts_nodash_with_tz,
            }
            context.update(context_from_server)
        # TODO: We should use/move TypeDict from airflow.utils.context.Context
        return context

    def render_templates(
        self, context: dict[str, Any] | None = None, jinja_env: jinja2.Environment | None = None
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
        original_task.render_template_fields(context, jinja_env)
        # TODO: Add support for rendering templates in the MappedOperator
        # if isinstance(self.task, MappedOperator):
        #     self.task = context["ti"].task

        return original_task

    def xcom_pull(
        self,
        task_ids: str | Iterable[str] | None = None,  # TODO: Simplify to a single task_id? (breaking change)
        dag_id: str | None = None,
        key: str = "return_value",  # TODO: Make this a constant (``XCOM_RETURN_KEY``)
        include_prior_dates: bool = False,  # TODO: Add support for this
        *,
        map_indexes: int | Iterable[int] | None = None,
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
            task_ids = self.task_id
        elif not isinstance(task_ids, str) and isinstance(task_ids, Iterable):
            # TODO: Handle multiple task_ids or remove support
            raise NotImplementedError("Multiple task_ids are not supported yet")

        if map_indexes is None:
            map_indexes = self.map_index
        elif isinstance(map_indexes, Iterable):
            # TODO: Handle multiple map_indexes or remove support
            raise NotImplementedError("Multiple map_indexes are not supported yet")

        log = structlog.get_logger(logger_name="task")
        SUPERVISOR_COMMS.send_request(
            log=log,
            msg=GetXCom(
                key=key,
                dag_id=dag_id,
                task_id=task_ids,
                run_id=run_id,
                map_index=map_indexes,
            ),
        )

        msg = SUPERVISOR_COMMS.get_message()
        if TYPE_CHECKING:
            assert isinstance(msg, XComResult)

        if msg.value is not None:
            from airflow.models.xcom import XCom

            # TODO: Move XCom serialization & deserialization to Task SDK
            #   https://github.com/apache/airflow/issues/45231
            return XCom.deserialize_value(msg)  # type: ignore[arg-type]
        return default

    def xcom_push(self, key: str, value: Any):
        """
        Make an XCom available for tasks to pull.

        :param key: Key to store the value under.
        :param value: Value to store. Only be JSON-serializable may be used otherwise.
        """
        from airflow.models.xcom import XCom

        # TODO: Move XCom serialization & deserialization to Task SDK
        #   https://github.com/apache/airflow/issues/45231
        value = XCom.serialize_value(value)

        log = structlog.get_logger(logger_name="task")
        SUPERVISOR_COMMS.send_request(
            log=log,
            msg=SetXCom(
                key=key,
                value=value,
                dag_id=self.dag_id,
                task_id=self.task_id,
                run_id=self.run_id,
            ),
        )

    def get_relevant_upstream_map_indexes(
        self, upstream: BaseOperator, ti_count: int | None, session: Any
    ) -> int | range | None:
        # TODO: Implement this method
        return None


def parse(what: StartupDetails) -> RuntimeTaskInstance:
    # TODO: Task-SDK:
    # Using DagBag here is about 98% wrong, but it'll do for now

    from airflow.models.dagbag import DagBag

    bag = DagBag(
        dag_folder=what.file,
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
    if not isinstance(task, BaseOperator):
        raise TypeError(f"task is of the wrong type, got {type(task)}, wanted {BaseOperator}")

    return RuntimeTaskInstance.model_construct(
        **what.ti.model_dump(exclude_unset=True),
        task=task,
        _ti_context_from_server=what.ti_context,
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

    if isinstance(msg, StartupDetails):
        from setproctitle import setproctitle

        setproctitle(f"airflow worker -- {msg.ti.id}")

        log = structlog.get_logger(logger_name="task")
        # TODO: set the "magic loop" context vars for parsing
        ti = parse(msg)
        log.debug("DAG file parsed", file=msg.file)
    else:
        raise RuntimeError(f"Unhandled  startup message {type(msg)} {msg}")

    # TODO: Render fields here
    # 1. Implementing the part where we pull in the logic to render fields and add that here
    # for all operators, we should do setattr(task, templated_field, rendered_templated_field)
    # task.templated_fields should give all the templated_fields and each of those fields should
    # give the rendered values. task.templated_fields should already be in a JSONable format and
    # we should not have to handle that here.

    # 2. Once rendered, we call the `set_rtif` API to store the rtif in the metadata DB

    # so that we do not call the API unnecessarily
    if rendered_fields := _get_rendered_fields(ti.task):
        SUPERVISOR_COMMS.send_request(log=log, msg=SetRenderedFields(rendered_fields=rendered_fields))
    return ti, log


def _get_rendered_fields(task: BaseOperator) -> dict[str, JsonValue]:
    # TODO: Port one of the following to Task SDK
    #   airflow.serialization.helpers.serialize_template_field or
    #   airflow.models.renderedtifields.get_serialized_template_fields
    from airflow.serialization.helpers import serialize_template_field

    return {field: serialize_template_field(getattr(task, field), field) for field in task.template_fields}


def run(ti: RuntimeTaskInstance, log: Logger):
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
    try:
        # TODO: pre execute etc.
        # TODO: Get a real context object
        ti.task = ti.task.prepare_for_execution()
        context = ti.get_template_context()
        with set_current_context(context):
            jinja_env = ti.task.dag.get_template_env()
            ti.task = ti.render_templates(context=context, jinja_env=jinja_env)
            result = _execute_task(context, ti.task)

        _push_xcom_if_needed(result, ti)

        # TODO: Get things from _execute_task_with_callbacks
        #   - Clearing XCom
        #   - Update RTIF
        #   - Pre Execute
        #   etc
        msg = TaskState(state=TerminalTIState.SUCCESS, end_date=datetime.now(tz=timezone.utc))
    except TaskDeferred as defer:
        classpath, trigger_kwargs = defer.trigger.serialize()
        next_method = defer.method_name
        defer_timeout = defer.timeout
        msg = DeferTask(
            classpath=classpath,
            trigger_kwargs=trigger_kwargs,
            next_method=next_method,
            trigger_timeout=defer_timeout,
        )
    except AirflowSkipException:
        msg = TaskState(
            state=TerminalTIState.SKIPPED,
            end_date=datetime.now(tz=timezone.utc),
        )
    except AirflowRescheduleException as reschedule:
        msg = RescheduleTask(
            reschedule_date=reschedule.reschedule_date, end_date=datetime.now(tz=timezone.utc)
        )
    except (AirflowFailException, AirflowSensorTimeout):
        # If AirflowFailException is raised, task should not retry.
        # If a sensor in reschedule mode reaches timeout, task should not retry.

        # TODO: Handle fail_stop here: https://github.com/apache/airflow/issues/44951
        # TODO: Handle addition to Log table: https://github.com/apache/airflow/issues/44952
        msg = TaskState(
            state=TerminalTIState.FAIL_WITHOUT_RETRY,
            end_date=datetime.now(tz=timezone.utc),
        )
        # TODO: Run task failure callbacks here
    except (AirflowTaskTimeout, AirflowException):
        # We should allow retries if the task has defined it.
        msg = TaskState(
            state=TerminalTIState.FAILED,
            end_date=datetime.now(tz=timezone.utc),
        )
        # TODO: Run task failure callbacks here
    except AirflowException:
        # TODO: handle the case of up_for_retry here
        msg = TaskState(
            state=TerminalTIState.FAILED,
            end_date=datetime.now(tz=timezone.utc),
        )
    except AirflowTaskTerminated:
        # External state updates are already handled with `ti_heartbeat` and will be
        # updated already be another UI API. So, these exceptions should ideally never be thrown.
        # If these are thrown, we should mark the TI state as failed.
        msg = TaskState(
            state=TerminalTIState.FAIL_WITHOUT_RETRY,
            end_date=datetime.now(tz=timezone.utc),
        )
        # TODO: Run task failure callbacks here
    except SystemExit:
        # SystemExit needs to be retried if they are eligible.
        msg = TaskState(
            state=TerminalTIState.FAILED,
            end_date=datetime.now(tz=timezone.utc),
        )
        # TODO: Run task failure callbacks here
    except BaseException:
        # TODO: Run task failure callbacks here
        msg = TaskState(state=TerminalTIState.FAILED, end_date=datetime.now(tz=timezone.utc))
    if msg:
        SUPERVISOR_COMMS.send_request(msg=msg, log=log)


def _execute_task(context: Mapping[str, Any], task: BaseOperator):
    """Execute Task (optionally with a Timeout) and push Xcom results."""
    from airflow.exceptions import AirflowTaskTimeout

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


def _push_xcom_if_needed(result: Any, ti: RuntimeTaskInstance):
    """Push XCom values when task has ``do_xcom_push`` set to ``True`` and the task returns a result."""
    if ti.task.do_xcom_push:
        xcom_value = result
    else:
        xcom_value = None

    # If the task returns a result, push an XCom containing it.
    if xcom_value is None:
        return

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
    ti.xcom_push("return_value", result)


def finalize(log: Logger): ...


def main():
    # TODO: add an exception here, it causes an oof of a stack trace!

    global SUPERVISOR_COMMS
    SUPERVISOR_COMMS = CommsDecoder[ToTask, ToSupervisor](input=sys.stdin)
    try:
        ti, log = startup()
        run(ti, log)
        finalize(log)
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
