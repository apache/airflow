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

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from airflow.models.deadline import _fetch_from_db
from airflow.utils.log.logging_mixin import LoggingMixin


class BaseDeadlineReference(LoggingMixin, ABC):
    """Base class for all Deadline implementations."""

    # Set of required kwargs - subclasses should override this.
    required_kwargs: set[str] = set()

    def evaluate_with(self, **kwargs: Any) -> datetime:
        """Validate the provided kwargs and evaluate this deadline with the given conditions."""
        filtered_kwargs = {k: kwargs[k] for k in self.required_kwargs if k in kwargs}

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


class DeadlineReference:
    """
    The public interface class for all DeadlineReference options.

    This class provides a unified interface for working with Deadlines, supporting both
    calculated deadlines (which fetch values from the database) and fixed deadlines
    (which return a predefined datetime).

    ------
    Usage:
    ------

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

    3. Evaluating deadlines will ignore unexpected parameters:
       # For deadlines requiring parameters:
           deadline = DeadlineReference.DAGRUN_LOGICAL_DATE
           deadline.evaluate_with(dag_id=dag.dag_id)

       # For deadlines with no required parameters:
           deadline = DeadlineReference.FIXED_DATETIME(datetime(2025, 5, 4))
           deadline.evaluate_with()
    """

    DAGRUN_LOGICAL_DATE = DagRunLogicalDateDeadline()
    DAGRUN_QUEUED_AT = DagRunQueuedAtDeadline()

    @classmethod
    def FIXED_DATETIME(cls, datetime: datetime) -> FixedDatetimeDeadline:
        return FixedDatetimeDeadline(datetime)
