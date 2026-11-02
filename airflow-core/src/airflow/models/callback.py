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

import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from importlib import import_module
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable
from uuid import UUID

import structlog
import uuid6
from sqlalchemy import ForeignKey, Index, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

# Re-exporting as _accepts_context for backward compatibility
from airflow._shared.module_loading import (
    accepts_context as _accepts_context,  # noqa: F401
    qualname,
)
from airflow._shared.observability.metrics import stats
from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.executors.workloads import BaseWorkload
from airflow.executors.workloads.callback import (
    CALLBACK_KWARGS_ENCODING_KEY,
    CALLBACK_REQUEST_DATA_KEY,
    SERDE_KWARGS_ENCODING,
    START_DEADLINE_KEY,
    STARTED_AT_KEY,
    STARTED_BY_KEY,
    CallbackFetchMethod,
)
from airflow.models import Base
from airflow.models.base import StringID
from airflow.models.dagbundle import DagBundleModel
from airflow.utils.helpers import prune_dict
from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime
from airflow.utils.state import CallbackState


@dataclass(frozen=True, slots=True)
class CallbackKey:
    """Distinct key type for callbacks, preventing any bare string from passing isinstance checks."""

    id: str

    def __str__(self) -> str:
        return self.id


if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.callbacks.callback_requests import CallbackRequest
    from airflow.triggers.base import TriggerEvent

log = structlog.get_logger(__name__)


ACTIVE_STATES = frozenset((CallbackState.PENDING, CallbackState.QUEUED, CallbackState.RUNNING))
TERMINAL_STATES = frozenset((CallbackState.SUCCESS, CallbackState.FAILED))


def _format_kwargs_for_display(kwargs: dict | None) -> Any:
    from airflow.serialization.decoders import format_encoded_value_for_display

    return format_encoded_value_for_display(kwargs) if kwargs else ""


def _get_request_data(request: CallbackRequest) -> dict[str, str]:
    return {"req_class": request.__class__.__name__, CALLBACK_REQUEST_DATA_KEY: request.to_json()}


class CallbackType(str, Enum):
    """
    Types of Callbacks.

    Used for figuring out what class to instantiate during deserialization.
    """

    TRIGGERER = "triggerer"
    EXECUTOR = "executor"
    DAG_PROCESSOR = "dag_processor"


class CallbackDefinitionProtocol(Protocol):
    """Protocol for TaskSDK Callback definition."""

    def serialize(self) -> dict[str, Any]:
        """Serialize to a dictionary."""
        ...


@runtime_checkable
class ImportPathCallbackDefProtocol(CallbackDefinitionProtocol, Protocol):
    """Protocol for callbacks that use the import path fetch method."""

    path: str
    kwargs: dict


@runtime_checkable
class ImportPathExecutorCallbackDefProtocol(ImportPathCallbackDefProtocol, Protocol):
    """Protocol for callbacks that use the import path fetch method and have an executor attribute to specify the executor to run them on."""

    executor: str | None


@runtime_checkable
class ImportPathAsyncCallbackDefProtocol(ImportPathCallbackDefProtocol, Protocol):
    """Protocol for callbacks that use the import path fetch method and support trigger queue assignment."""

    queue: str | None


class Callback(Base, BaseWorkload):
    """Base class for callbacks."""

    __tablename__ = "callback"
    id: Mapped[UUID] = mapped_column(Uuid(), primary_key=True, default=uuid6.uuid7)

    # This is used by SQLAlchemy to be able to deserialize DB rows to subclasses
    __mapper_args__ = {
        "polymorphic_identity": "callback",
        "polymorphic_on": "type",
    }
    type: Mapped[str] = mapped_column(String(20), nullable=False)

    # Method used to fetch the callback, of type: CallbackFetchMethod
    fetch_method: Mapped[str] = mapped_column(String(20), nullable=False)

    # Used by subclasses to store information about how to run the callback
    data: Mapped[dict] = mapped_column(ExtendedJSON, nullable=False)

    # Used to route dag-processor callbacks to filter by bundle name.
    bundle_name: Mapped[str | None] = mapped_column(StringID(), nullable=True)

    # State of the Callback of type: CallbackState. Can be null for instances of DagProcessorCallback.
    state: Mapped[str | None] = mapped_column(String(10))

    # Return value of the callback if successful, otherwise exception details
    output: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Used for prioritization. Higher weight -> higher priority
    priority_weight: Mapped[int] = mapped_column(Integer, nullable=False)

    # Creation time of the callback
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)

    # Used for callbacks of type CallbackType.TRIGGERER
    trigger_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("trigger.id"), nullable=True)
    trigger = relationship("Trigger", back_populates="callback", uselist=False)

    __table_args__ = (
        Index(
            "idx_callback_trigger_id",
            trigger_id,
            postgresql_where=trigger_id.is_not(None),
            sqlite_where=trigger_id.is_not(None),
        ),
        # Every scheduler loop looks callbacks up by type and state, and routed ones by fetch method too.
        Index("idx_callback_type_state_fetch_method", "type", "state", "fetch_method"),
    )

    def __init__(self, priority_weight: int = 1, prefix: str = "", **kwargs):
        """
        Initialize a Callback. This is the base class so it shouldn't usually need to be initialized.

        :param priority_weight: Priority for callback execution (higher value -> higher priority)
        :param prefix: Optional prefix for metric names
        :param kwargs: Additional data emitted in metric tags
        """
        self.state = CallbackState.SCHEDULED
        self.priority_weight = priority_weight
        self.data = kwargs  # kwargs can be used to include additional info in metric tags
        if prefix:
            self.data["prefix"] = prefix

    def queue(self, *, session: Session) -> None:
        self.state = CallbackState.QUEUED

    def get_metric_info(self, status: CallbackState, result: Any, team_name: str | None = None) -> dict:
        tags = {"result": result, **self.data}
        tags.pop("prefix", None)
        tags.pop(CALLBACK_KWARGS_ENCODING_KEY, None)

        if "kwargs" in tags:
            from airflow.serialization.decoders import format_encoded_value_for_display

            # Remove the context (if exists) to keep the tags simple
            tags["kwargs"] = format_encoded_value_for_display(
                {k: v for k, v in tags["kwargs"].items() if k != "context"}
            )

        # Metric backends (statsd, OpenTelemetry) require tag values to be primitives.
        # OTel's aggregation key is built via ``frozenset(attributes.items())``, which
        # raises ``TypeError: unhashable type: 'dict'`` if a value is a dict/list. The
        # callback's ``result`` (passed in from a user callback) and ``kwargs`` are both
        # frequently dicts, so coerce any non-primitive tag value to a JSON string before
        # returning. Using ``default=str`` so values like ``datetime`` fall back cleanly.
        tags = {
            k: v
            if isinstance(v, (str, int, float, bool)) or v is None
            else json.dumps(v, default=str, sort_keys=True)
            for k, v in tags.items()
        }

        # team_name is omitted entirely when not in a multi-team deployment or when the
        # callback's bundle is not mapped to a team.
        tags = prune_dict({**tags, "team_name": team_name})

        prefix = self.data.get("prefix", "")
        name = f"{prefix}.callback_{status}" if prefix else f"callback_{status}"

        return {"stat": name, "tags": tags}

    def get_dag_id(self) -> str | None:
        """Return the DAG ID for scheduler routing."""
        return self.data.get("dag_id")

    def get_executor_name(self) -> str | None:
        """Return the executor name for scheduler routing."""
        return self.data.get("executor")

    @property
    def start_deadline(self) -> datetime | None:
        """Return when a routed callback must have started by, or ``None`` if it has no deadline."""
        value = self.data.get(START_DEADLINE_KEY)
        return datetime.fromisoformat(value) if value else None

    @property
    def started_at(self) -> datetime | None:
        """Return when the callback's worker exchanged its token, if it recorded that."""
        value = self.data.get(STARTED_AT_KEY)
        return datetime.fromisoformat(value) if value else None

    @property
    def started_by(self) -> dict[str, Any] | None:
        """Return the worker process that exchanged the callback's token, if it identified itself."""
        return self.data.get(STARTED_BY_KEY)

    def mark_started(self, when: datetime, *, started_by: dict[str, Any] | None = None) -> None:
        # Reassigned rather than mutated so the JSON column is flagged as changed.
        self.data = {**self.data, STARTED_AT_KEY: when.isoformat(), STARTED_BY_KEY: started_by}

    def get_callback_request(self) -> CallbackRequest | None:
        """Return the Dag processor callback request this callback stores, or ``None`` if it has none."""
        if (req_class := self.data.get("req_class")) is None:
            return None
        request_class = getattr(import_module("airflow.callbacks.callback_requests"), req_class)
        return request_class.from_json(self.data[CALLBACK_REQUEST_DATA_KEY])

    @staticmethod
    def create_from_sdk_def(callback_def: CallbackDefinitionProtocol, **kwargs) -> Callback:
        # Cannot check actual type using isinstance() because that would require SDK import
        match type(callback_def).__name__:
            case "AsyncCallback":
                if TYPE_CHECKING:
                    assert isinstance(callback_def, ImportPathAsyncCallbackDefProtocol)
                return TriggererCallback(callback_def, **kwargs)

            case "SyncCallback":
                if TYPE_CHECKING:
                    assert isinstance(callback_def, ImportPathExecutorCallbackDefProtocol)
                return ExecutorCallback(callback_def, fetch_method=CallbackFetchMethod.IMPORT_PATH, **kwargs)

            case _:
                raise ValueError(f"Cannot handle Callback of type {type(callback_def)}")


class TriggererCallback(Callback):
    """Callbacks that run on the Triggerer (must be async)."""

    __mapper_args__ = {"polymorphic_identity": CallbackType.TRIGGERER}

    def __init__(self, callback_def: ImportPathCallbackDefProtocol, **kwargs):
        """
        Initialize a TriggererCallback from a callback definition.

        :param callback_def: Callback definition with path and kwargs
        :param kwargs: Passed to parent Callback.__init__ (see base class for details)
        """
        super().__init__(**kwargs)
        self.fetch_method = CallbackFetchMethod.IMPORT_PATH
        self.data |= callback_def.serialize()

    def __repr__(self):
        return f"{self.data['path']}({_format_kwargs_for_display(self.data['kwargs'])}) on a triggerer"

    def queue(self, *, session: Session) -> None:
        from airflow.models.trigger import Trigger
        from airflow.serialization.encoders import encode_queued_callback_kwargs
        from airflow.triggers.callback import CallbackTrigger

        team_name: str | None = None
        if self.bundle_name and conf.getboolean("core", "multi_team"):
            team_name = DagBundleModel.get_team_name(self.bundle_name, session=session)

        if self.id is None:
            # Not flushed yet; the trigger records which callback it runs for.
            self.id = uuid6.uuid7()
        dag_run = ((self.data.get("kwargs") or {}).get("context") or {}).get("dag_run") or {}
        # The kwargs stay encoded: the triggerer decodes them when it creates the CallbackTrigger.
        self.trigger = Trigger.from_serialized_kwargs(
            classpath=qualname(CallbackTrigger),
            serialized_kwargs={
                "callback_path": self.data["path"],
                "callback_kwargs": encode_queued_callback_kwargs(self.data["kwargs"] or {}),
            },
            queue=self.data.get("queue"),
            dag_id=self.get_dag_id() or dag_run.get("dag_id"),
            owner_ref={"callback_id": str(self.id)},
        )
        self.trigger.team_name = team_name
        super().queue(session=session)

    def handle_event(self, event: TriggerEvent, session: Session):
        from airflow.triggers.callback import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY

        if (status := event.payload.get(PAYLOAD_STATUS_KEY)) and status in (ACTIVE_STATES | TERMINAL_STATES):
            self.state = status
            if status in TERMINAL_STATES:
                team_name: str | None = None
                if self.bundle_name and conf.getboolean("core", "multi_team"):
                    team_name = DagBundleModel.get_team_name(self.bundle_name, session=session)
                self.trigger = None
                self.output = event.payload.get(PAYLOAD_BODY_KEY)
                stats.incr(**self.get_metric_info(status, self.output, team_name=team_name))

            session.add(self)
        else:
            log.error("Unexpected event received: %s", event.payload)


class ExecutorCallback(Callback):
    """Callbacks that run on the executor."""

    __mapper_args__ = {"polymorphic_identity": CallbackType.EXECUTOR}

    def __init__(
        self,
        callback_def: ImportPathExecutorCallbackDefProtocol | None,
        fetch_method: CallbackFetchMethod,
        *,
        request: CallbackRequest | None = None,
        **kwargs,
    ):
        """
        Initialize an ExecutorCallback from a callback definition, or from a Dag processor callback request.

        :param callback_def: Callback definition with path, kwargs, and executor; ``None`` with ``request``
        :param fetch_method: Method to fetch the callback at runtime
        :param request: Dag processor callback request, for a callback the scheduler routes to an executor
        :param kwargs: Passed to parent Callback.__init__ (see base class for details)
        """
        from airflow.serialization.encoders import encode_stored_callback_kwargs

        super().__init__(**kwargs)
        self.fetch_method = fetch_method
        if request is not None:
            self.bundle_name = request.bundle_name
            self.data |= _get_request_data(request)
            return
        if callback_def is None:
            raise TypeError("An ExecutorCallback needs a callback definition or a callback request")
        self.data |= callback_def.serialize()
        # The kwargs are decoded by the callback runner on the worker, which needs the marker
        # to tell them apart from the already-decoded kwargs of callbacks stored before it.
        self.data["kwargs"] = encode_stored_callback_kwargs(self.data.get("kwargs") or {})
        self.data[CALLBACK_KWARGS_ENCODING_KEY] = SERDE_KWARGS_ENCODING

    @classmethod
    def from_dag_processor_request(cls, request: CallbackRequest) -> ExecutorCallback:
        """
        Store a Dag processor callback request for the scheduler to route to an executor.

        The callback waits SCHEDULED until a scheduler routes it: queues it, or fails it when no
        executor can run it. A scheduler with routing turned off makes it a Dag processor callback
        again instead. As an executor callback, no Dag processor claims it in the meantime, and
        schedulers from before routing leave it alone: they only queue PENDING ones.
        """
        return cls(None, CallbackFetchMethod.DAG_ATTRIBUTE, request=request)

    def __repr__(self):
        if self.fetch_method == CallbackFetchMethod.DAG_ATTRIBUTE:
            return (
                f"{self.data.get('req_class')} for Dag {self.get_dag_id()} "
                f"on {self.data.get('executor') or 'default'} executor"
            )
        return (
            f"{self.data['path']}({_format_kwargs_for_display(self.data['kwargs'])}) "
            f"on {self.data.get('executor', 'default')} executor"
        )


class DagProcessorCallback(Callback):
    """Used to store Dag Processor's callback requests in the DB."""

    __mapper_args__ = {"polymorphic_identity": CallbackType.DAG_PROCESSOR}

    def __init__(self, priority_weight: int, callback: CallbackRequest):
        """Initialize a DagProcessorCallback from a callback request."""
        super().__init__(priority_weight=priority_weight)

        self.fetch_method = CallbackFetchMethod.DAG_ATTRIBUTE
        self.state = None
        self.bundle_name = callback.bundle_name
        self.data |= _get_request_data(callback)

    def get_callback_request(self) -> CallbackRequest:
        if (request := super().get_callback_request()) is None:
            raise ValueError(f"Dag processor callback {self.id} stores no callback request")
        return request
