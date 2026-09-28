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
"""Single-scheduler experiment against registered inventory; no provider or source access."""

from __future__ import annotations

import fcntl
import sqlite3
import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.engine import make_url

from airflow._shared.observability.metrics import stats
from airflow.configuration import conf
from airflow.dag_processing.orchestrator import ParseOrchestrator
from airflow.dag_processing.parsing_metadata import MetadataConnection, MetadataOrchestrationStore

if TYPE_CHECKING:
    from collections.abc import Iterator


class ParsingStepBudgetExceeded(TimeoutError):
    """The parsing transaction exhausted its cooperative scheduler budget."""


class SchedulerParsingConfig(BaseModel):
    """Trusted, deliberately small configuration for the scheduler experiment."""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)
    route: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.-]+$")
    bundle: str = Field(min_length=1, max_length=128)
    capacity: int = Field(default=2, ge=1, le=32, strict=True)
    batch_size: int = Field(default=10, ge=1, le=10, strict=True)
    parse_interval: float = Field(default=30, ge=0.1)
    retry_interval: float = Field(default=5, ge=0.1)
    interval: float = Field(default=0.1, ge=0.05, le=60)
    step_budget: float = Field(default=0.025, ge=0.001, le=0.1)

    @field_validator("route")
    @classmethod
    def validate_route(cls, value: str) -> str:
        if value in {"default", "celery"}:
            raise ValueError("Use a dedicated parsing route")
        return value


class SchedulerOrchestrationStore(MetadataOrchestrationStore):
    """Reuse registration SQL with non-waiting, interruptible scheduler transactions."""

    deadline: float | None = None

    @contextmanager
    def _open_transaction(self) -> Iterator[MetadataConnection]:
        if self.deadline is None:
            with super()._open_transaction() as connection:
                yield connection
            return
        deadline = self.deadline
        if time.monotonic() >= deadline:
            raise ParsingStepBudgetExceeded("Parsing step budget expired before database access")
        connection = sqlite3.connect(self.path, timeout=0, factory=MetadataConnection)
        connection.row_factory = sqlite3.Row
        connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            # Scheduler steps only register work. ORM metadata publication belongs to the API process.
            yield connection
            if time.monotonic() >= deadline:
                raise ParsingStepBudgetExceeded("Parsing step budget expired before commit")
            connection.commit()
        except BaseException:
            connection.set_progress_handler(None, 0)
            connection.rollback()
            raise
        finally:
            connection.close()


@dataclass(frozen=True)
class SchedulerParsingTick:
    """Record the outcome and elapsed time of one scheduler callback."""

    status: str
    duration: float
    workload_id: str | None = None


class SchedulerParsingHost:
    """Host one metadata-only orchestrator; the separately launched runner polls durable submissions."""

    def __init__(self, config_path: str):
        with Path(config_path).open("rb") as source:
            payload = source.read(8193)
        if len(payload) > 8192:
            raise ValueError("Scheduler parsing configuration exceeds 8 KiB")
        self.config = SchedulerParsingConfig.model_validate_json(payload)
        url = make_url(conf.get("database", "sql_alchemy_conn"))
        if url.get_backend_name() != "sqlite" or not url.database or url.database == ":memory:" or url.query:
            raise ValueError("Scheduler parsing currently requires a file-backed SQLite development database")
        if conf.getboolean("core", "multi_team"):
            raise ValueError("Scheduler parsing currently requires single-team configuration")
        self.path = Path(url.database).resolve(strict=True)
        self.interval = self.config.interval
        self._resources = ExitStack()
        self.store: SchedulerOrchestrationStore | None = None
        self.orchestrator: ParseOrchestrator | None = None

    def start(self) -> None:
        if self.store is not None:
            raise RuntimeError("Scheduler parsing host already started")
        try:
            lock = self._resources.enter_context(
                self.path.with_name(self.path.name + ".parsing.lock").open("a")
            )
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("Another executor parsing host owns this database") from None
            store = SchedulerOrchestrationStore(self.path)
            self._resources.callback(store.engine.dispose)
            store.restore_admissions(self.config.route)
            self.orchestrator = ParseOrchestrator(
                store,
                route=self.config.route,
                bundle=self.config.bundle,
                capacity=self.config.capacity,
                batch_size=self.config.batch_size,
                parse_interval=self.config.parse_interval,
                retry_interval=self.config.retry_interval,
            )
            self.store = store
        except BaseException:
            self.close()
            raise

    def tick(self) -> SchedulerParsingTick:
        if self.store is None or self.orchestrator is None:
            raise RuntimeError("Start the scheduler parsing host before ticking")
        started = time.monotonic()
        self.store.deadline = started + self.config.step_budget
        status, workload_id = "failed", None
        try:
            result = self.orchestrator.step(limit=self.config.batch_size)
            workload_id = result.workload_id
            status = "admitted" if workload_id is not None else "idle"
        except ParsingStepBudgetExceeded:
            status = "deferred"
        except sqlite3.OperationalError as error:
            if str(error) not in {"database is locked", "database table is locked", "interrupted"}:
                raise
            status = "deferred"
        finally:
            self.store.deadline = None
            duration = time.monotonic() - started
            stats.timing("scheduler.parsing_step_duration", duration * 1000)
            stats.incr(f"scheduler.parsing_step_{status}")
        return SchedulerParsingTick(status, duration, workload_id)

    def close(self) -> None:
        self._resources.close()
        self.store = None
        self.orchestrator = None
