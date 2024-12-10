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
from typing import TYPE_CHECKING, TextIO

import attrs
import structlog
from pydantic import ConfigDict, TypeAdapter

from airflow.sdk.api.datamodels._generated import TaskInstance, TerminalTIState
from airflow.sdk.definitions.baseoperator import BaseOperator
from airflow.sdk.execution_time.comms import DeferTask, StartupDetails, TaskState, ToSupervisor, ToTask

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger


class RuntimeTaskInstance(TaskInstance):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task: BaseOperator


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

    return RuntimeTaskInstance.model_construct(**what.ti.model_dump(exclude_unset=True), task=task)


@attrs.define()
class CommsDecoder:
    """Handle communication between the task in this process and the supervisor parent process."""

    input: TextIO

    request_socket: FileIO = attrs.field(init=False, default=None)

    decoder: TypeAdapter[ToTask] = attrs.field(init=False, factory=lambda: TypeAdapter(ToTask))

    def get_message(self) -> ToTask:
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

    def send_request(self, log: Logger, msg: ToSupervisor):
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
SUPERVISOR_COMMS: CommsDecoder

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
        return ti, log
    else:
        raise RuntimeError(f"Unhandled  startup message {type(msg)} {msg}")

    # TODO: Render fields here


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
        ti.task.execute({"task_instance": ti})  # type: ignore[attr-defined]
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
    except AirflowRescheduleException:
        ...
    except (AirflowFailException, AirflowSensorTimeout):
        # If AirflowFailException is raised, task should not retry.
        ...
    except (AirflowTaskTimeout, AirflowException, AirflowTaskTerminated):
        ...
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
    SUPERVISOR_COMMS = CommsDecoder(input=sys.stdin)
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
