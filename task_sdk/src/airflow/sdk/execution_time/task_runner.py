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
    RescheduleTask,
    SetRenderedFields,
    StartupDetails,
    TaskState,
    ToSupervisor,
    ToTask,
)
from airflow.sdk.execution_time.context import ConnectionAccessor

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger


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
            # "inlet_events": InletEventsAccessors(task.inlets, session=session),
            # "macros": macros,
            # "params": validated_params,
            # "prev_data_interval_start_success": get_prev_data_interval_start_success(),
            # "prev_data_interval_end_success": get_prev_data_interval_end_success(),
            # "prev_start_date_success": get_prev_start_date_success(),
            # "prev_end_date_success": get_prev_end_date_success(),
            # "test_mode": task_instance.test_mode,
            # "triggering_asset_events": lazy_object_proxy.Proxy(get_triggering_events),
            # "var": {
            #     "json": VariableAccessor(deserialize_json=True),
            #     "value": VariableAccessor(deserialize_json=False),
            # },
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
        return context

    def xcom_pull(self, *args, **kwargs): ...

    def xcom_push(self, *args, **kwargs): ...


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
        # TODO next_method to support resuming from deferred
        # TODO: Get a real context object
        ti.task = ti.task.prepare_for_execution()
        context = ti.get_template_context()
        ti.task.execute(context)  # type: ignore[attr-defined]
        msg = TaskState(state=TerminalTIState.SUCCESS, end_date=datetime.now(tz=timezone.utc))
    except TaskDeferred as defer:
        classpath, trigger_kwargs = defer.trigger.serialize()
        next_method = defer.method_name
        timeout = defer.timeout
        msg = DeferTask(
            classpath=classpath,
            trigger_kwargs=trigger_kwargs,
            next_method=next_method,
            trigger_timeout=timeout,
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
            state=TerminalTIState.FAILED,
            end_date=datetime.now(tz=timezone.utc),
        )

        # TODO: Run task failure callbacks here
    except (AirflowTaskTimeout, AirflowException):
        # TODO: handle the case of up_for_retry here
        ...
    except AirflowTaskTerminated:
        # External state updates are already handled with `ti_heartbeat` and will be
        # updated already be another UI API. So, these exceptions should ideally never be thrown.
        # If these are thrown, we should mark the TI state as failed.
        msg = TaskState(
            state=TerminalTIState.FAILED,
            end_date=datetime.now(tz=timezone.utc),
        )
        # TODO: Run task failure callbacks here
    except SystemExit:
        ...
    except BaseException:
        # TODO: Handle TI handle failure
        raise

    if msg:
        SUPERVISOR_COMMS.send_request(msg=msg, log=log)


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
