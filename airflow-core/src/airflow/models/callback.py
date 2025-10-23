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

from datetime import datetime
from enum import Enum
from importlib import import_module
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import structlog
import uuid6
from sqlalchemy import ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy_utils import UUIDType

from airflow._shared.timezones import timezone
from airflow.models import Base
from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime, mapped_column

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.callbacks.callback_requests import CallbackRequest
    from airflow.triggers.base import TriggerEvent

log = structlog.get_logger(__name__)


class CallbackState(str, Enum):
    """All possible states of callbacks."""

    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class CallbackType(str, Enum):
    """
    Types of Callbacks.

    Used for figuring out what class to instantiate while deserialization.
    """

    TRIGGERER = "triggerer"
    EXECUTOR = "executor"
    DAG_PROCESSOR = "dag_processor"


class CallbackFetchMethod(str, Enum):
    """Methods used to fetch callback at runtime."""

    # For future use once Dag Processor callbacks (on_success_callback/on_failure_callback) get moved to executors
    DAG_ATTRIBUTE = "dag_attribute"

    # For deadline callbacks since they import callbacks through the import path
    IMPORT_PATH = "import_path"


class CallbackDefinitionProtocol(Protocol):
    """Protocol for TaskSDK Callback definition."""

    def serialize(self) -> dict[str, Any]:
        """Serialize to a dictionary."""
        ...


@runtime_checkable
class ImportPathCallbackDefProtocol(CallbackDefinitionProtocol, Protocol):
    """Protocol for callbacks that use the import path fetch method."""

    path: str
    kwargs: dict | None


@runtime_checkable
class ImportPathExecutorCallbackDefProtocol(ImportPathCallbackDefProtocol, Protocol):
    """Protocol for callbacks that use the import path fetch method and have an executor attribute to specify the executor to run them on."""

    executor: str | None


class Callback(Base):
    """Base class for callbacks."""

    __tablename__ = "callback"

    id: Mapped[str] = mapped_column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)

    # This is used by SQLAlchemy to be able to deserialize DB rows to subclasses
    __mapper_args__ = {
        "polymorphic_identity": "callback",
        "polymorphic_on": "type",
    }
    type: Mapped[str] = mapped_column(String(20), nullable=False)

    # Method used to fetch the callback, of type: CallbackFetchMethod
    fetch_method: Mapped[str] = mapped_column(String(20), nullable=False)

    # Used by subclasses to store information about how to run the callback
    data: Mapped[dict] = mapped_column(ExtendedJSON)

    # State of the Callback of type: CallbackState. Can be null for instances of DagProcessorCallback.
    state: Mapped[str | None] = mapped_column(String(10))

    # Return value of the callback if successful, otherwise exception details
    output: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Used for prioritization. Higher weight -> higher priority
    priority_weight: Mapped[int] = mapped_column(Integer, nullable=False)

    # Creation time of the callback
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)

    # Used for callbacks of type CallbackType.TRIGGERER
    trigger_id: Mapped[int] = mapped_column(Integer, ForeignKey("trigger.id"), nullable=True)
    trigger = relationship("Trigger", back_populates="callback", uselist=False)

    def __init__(self, priority_weight: int = 1):
        self.state = CallbackState.PENDING
        self.priority_weight = priority_weight

    def queue(self):
        self.state = CallbackState.QUEUED

    @staticmethod
    def create_from_sdk_def(callback_def: CallbackDefinitionProtocol) -> Callback:
        # Cannot check actual type using isinstance() because that would require SDK import
        match type(callback_def).__name__:
            case "AsyncCallback":
                if TYPE_CHECKING:
                    assert isinstance(callback_def, ImportPathCallbackDefProtocol)
                return TriggererCallback(callback_def)

            case "SyncCallback":
                if TYPE_CHECKING:
                    assert isinstance(callback_def, ImportPathExecutorCallbackDefProtocol)
                return ExecutorCallback(callback_def, fetch_method=CallbackFetchMethod.IMPORT_PATH)

            case _:
                raise ValueError(f"Cannot handle Callback of type {type(callback_def)}")


class TriggererCallback(Callback):
    """Callbacks that run on the Triggerer (must be async)."""

    __mapper_args__ = {"polymorphic_identity": CallbackType.TRIGGERER}

    def __init__(self, callback_def: ImportPathCallbackDefProtocol, **kwargs):
        super().__init__(**kwargs)
        self.fetch_method = CallbackFetchMethod.IMPORT_PATH
        self.data = callback_def.serialize()

    def queue(self):
        # TODO: queue the trigger
        super().queue()

    def handle_event(self, event: TriggerEvent, session: Session):
        # TODO: modify fields based on the event
        pass


class ExecutorCallback(Callback):
    """Callbacks that run on the executor."""

    __mapper_args__ = {"polymorphic_identity": CallbackType.EXECUTOR}

    def __init__(
        self, callback_def: ImportPathExecutorCallbackDefProtocol, fetch_method: CallbackFetchMethod, **kwargs
    ):
        super().__init__(**kwargs)
        self.fetch_method = fetch_method
        self.data = callback_def.serialize()


class DagProcessorCallback(Callback):
    """Used to store Dag Processor's callback requests in the DB."""

    __mapper_args__ = {"polymorphic_identity": CallbackType.DAG_PROCESSOR}

    def __init__(self, priority_weight: int, callback: CallbackRequest):
        super().__init__(priority_weight=priority_weight)

        self.fetch_method = CallbackFetchMethod.DAG_ATTRIBUTE
        self.state = None
        self.data = {"req_class": callback.__class__.__name__, "req_data": callback.to_json()}

    def get_callback_request(self) -> CallbackRequest:
        module = import_module("airflow.callbacks.callback_requests")
        callback_request_class = getattr(module, self.data["req_class"])
        # Get the function (from the instance) that we need to call
        from_json = getattr(callback_request_class, "from_json")
        return from_json(self.data["req_data"])
