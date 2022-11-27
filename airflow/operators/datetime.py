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

import datetime
import warnings
from typing import Iterable

from airflow.exceptions import AirflowException, RemovedInAirflow3Warning
from airflow.operators.branch import BaseBranchOperator
from airflow.utils import timezone
from airflow.utils.context import Context


class BranchDateTimeOperator(BaseBranchOperator):
    """
    Branches into one of two lists of tasks depending on the current datetime.
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:BranchDateTimeOperator`

    True branch will be returned when ``datetime.datetime.now()`` falls below
    ``target_upper`` and above ``target_lower``.

    :param follow_task_ids_if_true: task id or task ids to follow if
        ``datetime.datetime.now()`` falls above target_lower and below ``target_upper``.
    :param follow_task_ids_if_false: task id or task ids to follow if
        ``datetime.datetime.now()`` falls below target_lower or above ``target_upper``.
    :param target_lower: target lower bound.
    :param target_upper: target upper bound.
    :param use_task_logical_date: If ``True``, uses task's logical date to compare with targets.
        Execution date is useful for backfilling. If ``False``, uses system's date.
    """

    def __init__(
        self,
        *,
        follow_task_ids_if_true: str | Iterable[str],
        follow_task_ids_if_false: str | Iterable[str],
        target_lower: datetime.datetime | datetime.time | None,
        target_upper: datetime.datetime | datetime.time | None,
        use_task_logical_date: bool = False,
        use_task_execution_date: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if target_lower is None and target_upper is None:
            raise AirflowException(
                "Both target_upper and target_lower are None. At least one "
                "must be defined to be compared to the current datetime"
            )

        self.target_lower = target_lower
        self.target_upper = target_upper
        self.follow_task_ids_if_true = follow_task_ids_if_true
        self.follow_task_ids_if_false = follow_task_ids_if_false
        self.use_task_logical_date = use_task_logical_date
        if use_task_execution_date:
            self.use_task_logical_date = use_task_execution_date
            warnings.warn(
                "Parameter ``use_task_execution_date`` is deprecated. Use ``use_task_logical_date``.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )

    def choose_branch(self, context: Context) -> str | Iterable[str]:
        if self.use_task_logical_date:
            now = context["logical_date"]
        else:
            now = timezone.coerce_datetime(timezone.utcnow())
        lower, upper = target_times_as_dates(now, self.target_lower, self.target_upper)
        lower = timezone.coerce_datetime(lower, self.dag.timezone)
        upper = timezone.coerce_datetime(upper, self.dag.timezone)

        if upper is not None and upper < now:
            return self.follow_task_ids_if_false

        if lower is not None and lower > now:
            return self.follow_task_ids_if_false

        return self.follow_task_ids_if_true


def target_times_as_dates(
    base_date: datetime.datetime,
    lower: datetime.datetime | datetime.time | None,
    upper: datetime.datetime | datetime.time | None,
):
    """Ensures upper and lower time targets are datetimes by combining them with base_date."""
    if isinstance(lower, datetime.datetime) and isinstance(upper, datetime.datetime):
        return lower, upper

    if lower is not None and isinstance(lower, datetime.time):
        lower = datetime.datetime.combine(base_date, lower)
    if upper is not None and isinstance(upper, datetime.time):
        upper = datetime.datetime.combine(base_date, upper)

    if lower is None or upper is None:
        return lower, upper

    if upper < lower:
        upper += datetime.timedelta(days=1)
    return lower, upper
