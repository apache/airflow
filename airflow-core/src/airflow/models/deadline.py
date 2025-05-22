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
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable

import sqlalchemy_jsonfield
import uuid6
from sqlalchemy import Column, ForeignKey, Index, Integer, String, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import relationship
from sqlalchemy_utils import UUIDType

from airflow.models.base import Base, StringID
from airflow.settings import json
from airflow.utils.module_loading import import_string, is_valid_dotpath
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.deadline_reference import DeadlineReference

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


class DeadlineAlert:
    """Store Deadline values needed to calculate the need-by timestamp and the callback information."""

    def __init__(
        self,
        reference: DeadlineReference,
        interval: timedelta,
        callback: Callable | str,
        callback_kwargs: dict | None = None,
    ):
        self.reference = reference
        self.interval = interval
        self.callback_kwargs = callback_kwargs
        self.callback = self.get_callback_path(callback)

    @staticmethod
    def get_callback_path(_callback: str | Callable) -> str:
        if callable(_callback):
            # Get the reference path to the callable in the form `airflow.models.deadline.get_from_db`
            return f"{_callback.__module__}.{_callback.__qualname__}"

        if not isinstance(_callback, str) or not is_valid_dotpath(_callback.strip()):
            raise ImportError(f"`{_callback}` doesn't look like a valid dot path.")

        stripped_callback = _callback.strip()

        try:
            # The provided callback is a string which appears to be a valid dotpath, attempt to import it.
            callback = import_string(stripped_callback)
        except ImportError as e:
            # Logging here instead of failing because it is possible that the code for the callable
            # exists somewhere other than on the DAG processor. We are making a best effort to validate,
            # but can't rule out that it may be available at runtime even if it can not be imported here.
            logger.debug(
                "Callback %s is formatted like a callable dotpath, but could not be imported.\n%s",
                stripped_callback,
                e,
            )
            return stripped_callback

        # If we get this far then the input is a string which can be imported, check if it is a callable.
        if not callable(callback):
            raise AttributeError(f"Provided callback {callback} is not callable.")

        return stripped_callback

    def serialize_deadline_alert(self):
        from airflow.serialization.serialized_objects import BaseSerialization

        return BaseSerialization.serialize(
            {
                "reference": self.reference,
                "interval": self.interval,
                "callback": self.callback,
                "callback_kwargs": self.callback_kwargs,
            }
        )


@provide_session
def _fetch_from_db(model_reference: Column, session=None, **conditions) -> datetime:
    """
    Fetch a datetime stored in the database.

    :param model_reference: SQLAlchemy Column reference (e.g., DagRun.logical_date, TaskInstance.queued_dttm, etc.)
    :param conditions: Key-value pairs which are passed to the WHERE clause.

    :param session: SQLAlchemy session (provided by decorator)
    """
    query = select(model_reference)

    for key, value in conditions.items():
        query = query.where(getattr(model_reference.class_, key) == value)

    # This should build a query similar to:
    # session.scalar(select(DagRun.logical_date).where(DagRun.dag_id == dag_id))
    logger.debug("db query: session.scalar(%s)", query)

    try:
        result = session.scalar(query)
    except SQLAlchemyError as e:
        logger.error("Database query failed: (%s)", str(e))
        raise

    if result is None:
        message = "No matching record found in the database."
        logger.error(message)
        raise ValueError(message)

    return result
