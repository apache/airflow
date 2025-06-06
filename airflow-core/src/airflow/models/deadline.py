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

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

import sqlalchemy_jsonfield
import uuid6
from sqlalchemy import Column, ForeignKey, Index, Integer, String, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import relationship
from sqlalchemy_utils import UUIDType

from airflow.models.base import Base, StringID
from airflow.settings import json
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


logger = logging.getLogger(__name__)


class Deadline(Base):
    """A Deadline is a 'need-by' date which triggers a callback if the provided time has passed."""

    __tablename__ = "deadline"

    id = Column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)

    # If the Deadline Alert is for a DAG, store the DAG ID and Run ID from the dag_run.
    dag_id = Column(StringID(), ForeignKey("dag.dag_id", ondelete="CASCADE"))
    dagrun_id = Column(Integer, ForeignKey("dag_run.id", ondelete="CASCADE"))

    # The time after which the Deadline has passed and the callback should be triggered.
    deadline = Column(UtcDateTime, nullable=False)
    # The Callback to be called when the Deadline has passed.
    callback = Column(String(500), nullable=False)
    # Serialized kwargs to pass to the callback.
    callback_kwargs = Column(sqlalchemy_jsonfield.JSONField(json=json))

    dagrun = relationship("DagRun", back_populates="deadlines")

    __table_args__ = (Index("deadline_idx", deadline, unique=False),)

    def __init__(
        self,
        deadline: datetime,
        callback: str,
        callback_kwargs: dict | None = None,
        dag_id: str | None = None,
        dagrun_id: int | None = None,
    ):
        super().__init__()
        self.deadline = deadline
        self.callback = callback
        self.callback_kwargs = callback_kwargs
        self.dag_id = dag_id
        self.dagrun_id = dagrun_id

    def __repr__(self):
        def _determine_resource() -> tuple[str, str]:
            """Determine the type of resource based on which values are present."""
            if self.dag_id and self.dagrun_id:
                # The deadline is for a dagrun:
                return "DagRun", f"Dag: {self.dag_id} Run: {self.dagrun_id}"

            return "Unknown", ""

        resource_type, resource_details = _determine_resource()
        callback_kwargs = json.dumps(self.callback_kwargs) if self.callback_kwargs else ""

        return (
            f"[{resource_type} Deadline] {resource_details} needed by "
            f"{self.deadline} or run: {self.callback}({callback_kwargs})"
        )

    @classmethod
    @provide_session
    def add_deadline(cls, deadline: Deadline, session: Session = NEW_SESSION):
        """Add the provided deadline to the table."""
        session.add(deadline)


class ReferenceModels:
    """
    Store the implementations for the different Deadline References.

    After adding the implementations here, all DeadlineReferences should be added
    to the user interface in airflow.sdk.definitions.deadline.DeadlineReference
    """

    class BaseDeadlineReference(LoggingMixin, ABC):
        """Base class for all Deadline implementations."""

        # Set of required kwargs - subclasses should override this.
        required_kwargs: set[str] = set()

        def evaluate_with(self, **kwargs: Any) -> datetime:
            """Validate the provided kwargs and evaluate this deadline with the given conditions."""
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in self.required_kwargs}

            if missing_kwargs := self.required_kwargs - filtered_kwargs.keys():
                raise ValueError(
                    f"{self.__class__.__name__} is missing required parameters: {', '.join(missing_kwargs)}"
                )

            if extra_kwargs := kwargs.keys() - filtered_kwargs.keys():
                self.log.debug("Ignoring unexpected parameters: %s", ", ".join(extra_kwargs))

            return self._evaluate_with(**filtered_kwargs)

        @abstractmethod
        def _evaluate_with(self, **kwargs: Any) -> datetime:
            """Must be implemented by subclasses to perform the actual evaluation."""
            raise NotImplementedError

    @dataclass
    class FixedDatetimeDeadline(BaseDeadlineReference):
        """A deadline that always returns a fixed datetime."""

        _datetime: datetime

        def _evaluate_with(self, **kwargs: Any) -> datetime:
            return self._datetime

    class DagRunLogicalDateDeadline(BaseDeadlineReference):
        """A deadline that returns a DagRun's logical date."""

        required_kwargs = {"dag_id"}

        def _evaluate_with(self, **kwargs: Any) -> datetime:
            from airflow.models import DagRun

            return _fetch_from_db(DagRun.logical_date, **kwargs)

    class DagRunQueuedAtDeadline(BaseDeadlineReference):
        """A deadline that returns when a DagRun was queued."""

        required_kwargs = {"dag_id"}

        def _evaluate_with(self, **kwargs: Any) -> datetime:
            from airflow.models import DagRun

            return _fetch_from_db(DagRun.queued_at, **kwargs)


DeadlineReferenceType = ReferenceModels.BaseDeadlineReference


@provide_session
def _fetch_from_db(model_reference: Column, session=None, **conditions) -> datetime:
    """
    Fetch a datetime value from the database using the provided model reference and filtering conditions.

    For example, to fetch a TaskInstance's start_date:
        _fetch_from_db(
            TaskInstance.start_date, dag_id='example_dag', task_id='example_task', run_id='example_run'
        )

    This generates SQL equivalent to:
        SELECT start_date
        FROM task_instance
        WHERE dag_id = 'example_dag'
            AND task_id = 'example_task'
            AND run_id = 'example_run'

    :param model_reference: SQLAlchemy Column to select (e.g., DagRun.logical_date, TaskInstance.start_date)
    :param conditions: Filtering conditions applied as equality comparisons in the WHERE clause.
                       Multiple conditions are combined with AND.
    :param session: SQLAlchemy session (auto-provided by decorator)
    """
    query = select(model_reference)

    for key, value in conditions.items():
        query = query.where(getattr(model_reference.class_, key) == value)

    compiled_query = query.compile(compile_kwargs={"literal_binds": True})
    pretty_query = "\n    ".join(str(compiled_query).splitlines())
    logger.debug(
        "Executing query:\n    %r\nAs SQL:\n    %s",
        query,
        pretty_query,
    )

    try:
        result = session.scalar(query)
    except SQLAlchemyError:
        logger.exception("Database query failed.")
        raise

    if result is None:
        message = f"No matching record found in the database for query:\n    {pretty_query}"
        logger.error(message)
        raise ValueError(message)

    return result
