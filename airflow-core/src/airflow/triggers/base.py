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
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Annotated, Any

import structlog
from pydantic import (
    BaseModel,
    Discriminator,
    JsonValue,
    Tag,
    model_serializer,
)

from airflow.sdk.definitions._internal.templater import Templater
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import TaskInstanceState

log = structlog.get_logger(logger_name=__name__)

if TYPE_CHECKING:
    from typing import TypeAlias

    import jinja2

    from airflow.models.mappedoperator import MappedOperator
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk.definitions.context import Context
    from airflow.serialization.serialized_objects import SerializedBaseOperator

    Operator: TypeAlias = MappedOperator | SerializedBaseOperator


@dataclass
class StartTriggerArgs:
    """Arguments required for start task execution from triggerer."""

    trigger_cls: str
    next_method: str
    trigger_kwargs: dict[str, Any] | None = None
    next_kwargs: dict[str, Any] | None = None
    timeout: timedelta | None = None


class BaseTrigger(abc.ABC, Templater, LoggingMixin):
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
        super().__init__()
        # these values are set by triggerer when preparing to run the instance
        # when run, they are injected into logger record.
        self._task_instance = None
        self.trigger_id = None
        self.template_fields = ()
        self.template_ext = ()

    def _set_context(self, context):
        """Part of LoggingMixin and used mainly for configuration of task logging; not used for triggers."""
        pass

    @property
    def task(self) -> Operator | None:
        if self.task_instance:
            return self.task_instance.task
        return None

    @property
    def task_instance(self) -> TaskInstance | None:
        return self._task_instance

    @task_instance.setter
    def task_instance(self, value: TaskInstance | None) -> None:
        self._task_instance = value
        if self.task:
            self.template_fields = self.task.template_fields
            self.template_ext = self.task.template_ext

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        """
        Template all attributes listed in *self.template_fields*.

        This mutates the attributes in-place and is irreversible.

        :param context: Context dict with values to apply on content.
        :param jinja_env: Jinja's environment to use for rendering.
        """
        # We only need to render templated fields if templated fields are part of the start_trigger_args
        for attr_name in self.template_fields:
            value = getattr(self, attr_name, None)

            if value:
                try:
                    rendered_content = self.render_template(value, context, jinja_env)
                except Exception:
                    # TODO: Mask the value. Depends on https://github.com/apache/airflow/issues/45438
                    if self.task:
                        self.log.exception(
                            "Exception rendering Jinja template for task '%s', field '%s'. Template: %r",
                            self.task.task_id,
                            attr_name,
                            value,
                        )
                    raise
                else:
                    setattr(self, attr_name, rendered_content)

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

    @staticmethod
    def repr(classpath: str, kwargs: dict[str, Any]):
        kwargs_str = ", ".join(f"{k}={v}" for k, v in kwargs.items())
        return f"<{classpath} {kwargs_str}>"

    def __repr__(self) -> str:
        classpath, kwargs = self.serialize()
        return self.repr(classpath, kwargs)


class BaseEventTrigger(BaseTrigger):
    """
    Base class for triggers used to schedule DAGs based on external events.

    ``BaseEventTrigger`` is a subclass of ``BaseTrigger`` designed to identify triggers compatible with
    event-driven scheduling.
    """

    @staticmethod
    def hash(classpath: str, kwargs: dict[str, Any]) -> int:
        """
        Return the hash of the trigger classpath and kwargs. This is used to uniquely identify a trigger.

        We do not want to have this logic in ``BaseTrigger`` because, when used to defer tasks, 2 triggers
        can have the same classpath and kwargs. This is not true for event driven scheduling.
        """
        from airflow.serialization.serialized_objects import BaseSerialization

        return hash((classpath, json.dumps(BaseSerialization.serialize(kwargs)).encode("utf-8")))


class TriggerEvent(BaseModel):
    """
    Something that a trigger can fire when its conditions are met.

    Events must have a uniquely identifying value that would be the same
    wherever the trigger is run; this is to ensure that if the same trigger
    is being run in two locations (for HA reasons) that we can deduplicate its
    events.
    """

    payload: Any = None
    """
    The payload for the event to send back to the task.

    Must be natively JSON-serializable, or registered with the airflow serialization code.
    """

    def __init__(self, payload, **kwargs):
        super().__init__(payload=payload, **kwargs)

    def __repr__(self) -> str:
        return f"TriggerEvent<{self.payload!r}>"


class BaseTaskEndEvent(TriggerEvent):
    """
    Base event class to end the task without resuming on worker.

    :meta private:
    """

    task_instance_state: TaskInstanceState
    xcoms: dict[str, JsonValue] | None = None

    def __init__(self, *, xcoms: dict[str, JsonValue] | None = None, **kwargs) -> None:
        """
        Initialize the class with the specified parameters.

        :param xcoms: A dictionary of XComs or None.
        :param kwargs: Additional keyword arguments.
        """
        if "payload" in kwargs:
            raise ValueError("Param 'payload' not supported for this class.")
        # Yes this is _odd_. It's to support both constructor from users of
        # `TaskSuccessEvent(some_xcom_value)` and deserialization by pydantic.
        state = kwargs.pop("task_instance_state", self.__pydantic_fields__["task_instance_state"].default)
        super().__init__(payload=str(state), task_instance_state=state, **kwargs)
        self.xcoms = xcoms

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        # We need to customize the serialized schema so it works for the custom constructor we have to keep
        # the interface to this class "nice"
        return {"task_instance_state": self.task_instance_state, "xcoms": self.xcoms}


class TaskSuccessEvent(BaseTaskEndEvent):
    """Yield this event in order to end the task successfully."""

    task_instance_state: TaskInstanceState = TaskInstanceState.SUCCESS


class TaskFailedEvent(BaseTaskEndEvent):
    """Yield this event in order to end the task with failure."""

    task_instance_state: TaskInstanceState = TaskInstanceState.FAILED


class TaskSkippedEvent(BaseTaskEndEvent):
    """Yield this event in order to end the task with status 'skipped'."""

    task_instance_state: TaskInstanceState = TaskInstanceState.SKIPPED


def trigger_event_discriminator(v):
    if isinstance(v, dict):
        return v.get("task_instance_state", "_event_")
    if isinstance(v, TriggerEvent):
        return getattr(v, "task_instance_state", "_event_")


DiscrimatedTriggerEvent = Annotated[
    Annotated[TriggerEvent, Tag("_event_")]
    | Annotated[TaskSuccessEvent, Tag(TaskInstanceState.SUCCESS)]
    | Annotated[TaskFailedEvent, Tag(TaskInstanceState.FAILED)]
    | Annotated[TaskSkippedEvent, Tag(TaskInstanceState.SKIPPED)],
    Discriminator(trigger_event_discriminator),
]
