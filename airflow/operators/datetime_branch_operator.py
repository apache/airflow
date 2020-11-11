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

import datetime
from typing import Dict, Iterable, Optional, Union

from airflow.exceptions import AirflowException
from airflow.operators.branch_operator import BaseBranchOperator
from airflow.utils import timezone
from airflow.utils.decorators import apply_defaults


class DateTimeBranchOperator(BaseBranchOperator):
    """
    Branches into one of two lists of tasks depending on the current datetime.

    True branch will be returned when `datetime.datetime.now()` falls below
    `target_upper` and above `target_lower`.

    :param follow_task_ids_if_true: task id or task ids to follow if
        `datetime.datetime.now()` falls above target_lower and below `target_upper`.
    :type follow_task_ids_if_true: str or list[str]
    :param follow_task_ids_if_false: task id or task ids to follow if
        `datetime.datetime.now()` falls below target_lower or above `target_upper`.
    :type follow_task_ids_if_false: str or list[str]
    :param target_lower: target lower bound.
    :type target_lower: Optional[datetime.datetime]
    :param target_upper: target upper bound.
    :type target_upper: Optional[datetime.datetime]
    """

    @apply_defaults
    def __init__(
        self,
        *,
        follow_task_ids_if_true: Union[str, Iterable[str]],
        follow_task_ids_if_false: Union[str, Iterable[str]],
        target_lower: Optional[datetime.datetime],
        target_upper: Optional[datetime.datetime],
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

    def choose_branch(self, context: Dict) -> Union[str, Iterable[str]]:
        now = timezone.make_naive(timezone.utcnow(), self.dag.timezone)

        if self.target_upper is not None and self.target_upper < now:
            return self.follow_task_ids_if_false

        if self.target_lower is not None and self.target_lower > now:
            return self.follow_task_ids_if_false

        return self.follow_task_ids_if_true
