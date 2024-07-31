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

import abc
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, AsyncIterator

from airflow.callbacks.callback_requests import TaskCallbackRequest
from airflow.callbacks.database_callback_sink import DatabaseCallbackSink
from airflow.models.taskinstance import SimpleTaskInstance
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models import TaskInstance

log = logging.getLogger(__name__)


@dataclass
class StartTriggerArgs:
    """Arguments required for start task execution from triggerer."""

    trigger_cls: str
    next_method: str
    trigger_kwargs: dict[str, Any] | None = None
    next_kwargs: dict[str, Any] | None = None
    timeout: timedelta | None = None


class BaseTrigger(abc.ABC, LoggingMixin):
    """
    Base class for all triggers.

    A trigger has two contexts it can exist in:

     - Inside an Operator, when it's passed to TaskDeferred
     - Actively running in a trigger worker

    We use the same class for both situations, and rely on all Trigger classes
    to be able to return the arguments (possible to encode with Airflow-JSON) that will
    let them be re-instantiated elsewhere.
    """

    def __init__(self, **kwargs):
        # these values are set by triggerer when preparing to run the instance
        # when run, they are injected into logger record.
        self.task_instance = None
        self.trigger_id = None

    def _set_context(self, context):
        """Part of LoggingMixin and used mainly for configuration of task logging; not used for triggers."""
        raise NotImplementedError

    @abc.abstractmethod
    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Return the information needed to reconstruct this Trigger.

        :return: Tuple of (class path, keyword arguments needed to re-instantiate).
        """
        raise NotImplementedError("Triggers must implement serialize()")

    @abc.abstractmethod
    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Run the trigger in an asynchronous context.

        The trigger should yield an Event whenever it wants to fire off
        an event, and return None if it is finished. Single-event triggers
        should thus yield and then immediately return.

        If it yields, it is likely that it will be resumed very quickly,
        but it may not be (e.g. if the workload is being moved to another
        triggerer process, or a multi-event trigger was being used for a
        single-event task defer).

        In either case, Trigger classes should assume they will be persisted,
        and then rely on cleanup() being called when they are no longer needed.
        """
        raise NotImplementedError("Triggers must implement run()")
        yield  # To convince Mypy this is an async iterator.

    async def cleanup(self) -> None:
        """
        Cleanup the trigger.

        Called when the trigger is no longer needed, and it's being removed
        from the active triggerer process.

        This method follows the async/await pattern to allow to run the cleanup
        in triggerer main event loop. Exceptions raised by the cleanup method
        are ignored, so if you would like to be able to debug them and be notified
        that cleanup method failed, you should wrap your code with try/except block
        and handle it appropriately (in async-compatible way).
        """

    def __repr__(self) -> str:
        classpath, kwargs = self.serialize()
        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        return f"<{classpath} {kwargs_str}>"


class TriggerEvent:
    """
    Something that a trigger can fire when its conditions are met.

    Events must have a uniquely identifying value that would be the same
    wherever the trigger is run; this is to ensure that if the same trigger
    is being run in two locations (for HA reasons) that we can deduplicate its
    events.
    """

    def __init__(self, payload: Any):
        self.payload = payload

    def __repr__(self) -> str:
        return f"TriggerEvent<{self.payload!r}>"

    def __eq__(self, other):
        if isinstance(other, TriggerEvent):
            return other.payload == self.payload
        return False

    @provide_session
    def handle_submit(self, *, task_instance: TaskInstance, session: Session = NEW_SESSION) -> None:
        """
        Handle the submit event for a given task instance.

        This function sets the next method and next kwargs of the task instance,
        as well as its state to scheduled. It also adds the event's payload
        into the kwargs for the task.

        :param task_instance: The task instance to handle the submit event for.
        :param session: The session to be used for the database callback sink.
        """
        # Get the next kwargs of the task instance, or an empty dictionary if it doesn't exist
        next_kwargs = task_instance.next_kwargs or {}

        # Add the event's payload into the kwargs for the task
        next_kwargs["event"] = self.payload

        # Update the next kwargs of the task instance
        task_instance.next_kwargs = next_kwargs

        # Remove ourselves as its trigger
        task_instance.trigger_id = None

        # Set the state of the task instance to scheduled
        task_instance.state = TaskInstanceState.SCHEDULED


class BaseTaskEndEvent(TriggerEvent):
    """
    Base event class to end the task without resuming on worker.

    :meta private:
    """

    task_instance_state: TaskInstanceState

    def __init__(self, *, xcoms: dict[str, Any] | None = None, **kwargs) -> None:
        """
        Initialize the class with the specified parameters.

        :param xcoms: A dictionary of XComs or None.
        :param kwargs: Additional keyword arguments.
        """
        if "payload" in kwargs:
            raise ValueError("Param 'payload' not supported for this class.")
        super().__init__(payload=self.task_instance_state)
        self.xcoms = xcoms

    @provide_session
    def handle_submit(self, *, task_instance: TaskInstance, session: Session = NEW_SESSION) -> None:
        """
        Submit event for the given task instance.

        Marks the task with the state `task_instance_state` and optionally pushes xcom if applicable.

        :param task_instance: The task instance to be submitted.
        :param session: The session to be used for the database callback sink.
        """
        # Mark the task with terminal state and prevent it from resuming on worker
        task_instance.trigger_id = None
        task_instance.state = self.task_instance_state
        self._submit_callback_if_necessary(task_instance=task_instance, session=session)
        self._push_xcoms_if_necessary(task_instance=task_instance)

    def _submit_callback_if_necessary(self, *, task_instance: TaskInstance, session) -> None:
        """Submit a callback request if the task state is SUCCESS or FAILED."""
        if self.task_instance_state in (TaskInstanceState.SUCCESS, TaskInstanceState.FAILED):
            request = TaskCallbackRequest(
                full_filepath=task_instance.dag_model.fileloc,
                simple_task_instance=SimpleTaskInstance.from_ti(task_instance),
                task_callback_type=self.task_instance_state,
            )
            log.info("Sending callback: %s", request)
            try:
                DatabaseCallbackSink().send(callback=request, session=session)
            except Exception:
                log.exception("Failed to send callback.")

    def _push_xcoms_if_necessary(self, *, task_instance: TaskInstance) -> None:
        """Pushes XComs to the database if they are provided."""
        if self.xcoms:
            for key, value in self.xcoms.items():
                task_instance.xcom_push(key=key, value=value)


class TaskSuccessEvent(BaseTaskEndEvent):
    """Yield this event in order to end the task successfully."""

    task_instance_state = TaskInstanceState.SUCCESS


class TaskFailedEvent(BaseTaskEndEvent):
    """Yield this event in order to end the task with failure."""

    task_instance_state = TaskInstanceState.FAILED


class TaskSkippedEvent(BaseTaskEndEvent):
    """Yield this event in order to end the task with status 'skipped'."""

    task_instance_state = TaskInstanceState.SKIPPED
