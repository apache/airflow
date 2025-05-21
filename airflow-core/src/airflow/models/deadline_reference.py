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

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from sqlalchemy import Column, select
from sqlalchemy.exc import SQLAlchemyError

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session


class CalculatedDeadline(LoggingMixin, Enum):
    """
    Implementation class for deadlines which are calculated at runtime.

    Do not instantiate directly. Instead, use DeadlineReference:

        deadline = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
            interval=timedelta(hours=1),
            callback=hello_callback,
        )
    """

    DAGRUN_LOGICAL_DATE = "dagrun_logical_date"
    DAGRUN_QUEUED_AT = "dagrun_queued_at"

    def evaluate_with(self, **kwargs):
        """Call the method in the enum's value with the provided kwargs."""
        return getattr(self, self.value)(**kwargs)

    def evaluate(self):
        """Call evaluate_with() without any conditions, because it looks strange in use that way."""
        return self.evaluate_with()

    @provide_session
    def _fetch_from_db(self, model_reference: Column, session=None, **conditions) -> datetime:
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
        self.log.debug("db query: session.scalar(%s)", query)

        try:
            result = session.scalar(query)
        except SQLAlchemyError as e:
            self.log.error("Database query failed: (%s)", str(e))
            raise

        if result is None:
            message = "No matching record found in the database."
            self.log.error(message)
            raise ValueError(message)

        return result

    def dagrun_logical_date(self, dag_id: str) -> datetime:
        from airflow.models import DagRun

        return self._fetch_from_db(DagRun.logical_date, dag_id=dag_id)

    def dagrun_queued_at(self, dag_id: str) -> datetime:
        from airflow.models import DagRun

        return self._fetch_from_db(DagRun.queued_at, dag_id=dag_id)


@dataclass
class FixedDatetimeDeadline(LoggingMixin):
    """Implementation class for fixed datetime deadlines."""

    _datetime: datetime

    def evaluate_with(self, **kwargs) -> datetime:
        """
        Evaluate this deadline reference with the given kwargs.

        Ignores all kwargs as fixed deadlines don't need any parameters.
        """
        if kwargs:
            self.log.debug("Fixed Datetime Deadlines do not accept conditions, ignoring kwargs: %s", kwargs)
        return self._datetime

    def evaluate(self) -> datetime:
        """Evaluate this deadline reference with no parameters."""
        return self.evaluate_with()


class DeadlineReference:
    """
    The public interface class for all DeadlineReference options.

    This class provides a unified interface for working with Deadlines, supporting both
    calculated deadlines (which fetch values from the database) and fixed deadlines
    (which return a predefined datetime).

    ------
    Usage:
    -  ------

    1. Example deadline references:
       fixed = DeadlineReference.FIXED_DATETIME(datetime(2025, 5, 4))
       logical = DeadlineReference.DAGRUN_LOGICAL_DATE
       queued = DeadlineReference.DAGRUN_QUEUED_AT

    2. Using in a DAG:
       DAG(
           dag_id='dag_with_deadline',
           deadline=DeadlineAlert(
               reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
               interval=timedelta(hours=1),
               callback=hello_callback,
           )
       )

    3. Evaluating deadlines:
       # Calculated deadlines require conditions:
       logical = DeadlineReference.DAGRUN_LOGICAL_DATE
       logical.evaluate_with(dag_id=dag.dag_id)
       logical.evaluate() will raise TypeError - missing required argument

       # Fixed deadlines ignore conditions:
       fixed = DeadlineReference.FIXED_DATETIME(datetime(2025, 5, 4))
       fixed.evaluate() is shorthand for fixed.evaluate_with(), both will work.
    """

    DAGRUN_LOGICAL_DATE = CalculatedDeadline.DAGRUN_LOGICAL_DATE
    DAGRUN_QUEUED_AT = CalculatedDeadline.DAGRUN_QUEUED_AT

    @classmethod
    def FIXED_DATETIME(cls, datetime: datetime) -> FixedDatetimeDeadline:
        """Create a new fixed datetime deadline."""
        return FixedDatetimeDeadline(datetime)
