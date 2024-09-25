#
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
"""
Example DAG demonstrating the usage of BranchDayOfWeekOperator.
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay

with DAG(
    dag_id="example_weekday_branch_operator",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    schedule="@daily",
) as dag:
    # [START howto_operator_day_of_week_branch]
    empty_task_1 = EmptyOperator(task_id="branch_true")
    empty_task_2 = EmptyOperator(task_id="branch_false")
    empty_task_3 = EmptyOperator(task_id="branch_weekend")
    empty_task_4 = EmptyOperator(task_id="branch_mid_week")

    branch = BranchDayOfWeekOperator(
        task_id="make_choice",
        follow_task_ids_if_true="branch_true",
        follow_task_ids_if_false="branch_false",
        week_day="Monday",
    )
    branch_weekend = BranchDayOfWeekOperator(
        task_id="make_weekend_choice",
        follow_task_ids_if_true="branch_weekend",
        follow_task_ids_if_false="branch_mid_week",
        week_day={WeekDay.SATURDAY, WeekDay.SUNDAY},
    )

    # Run empty_task_1 if branch executes on Monday, empty_task_2 otherwise
    branch >> [empty_task_1, empty_task_2]
    # Run empty_task_3 if it's a weekend, empty_task_4 otherwise
    empty_task_2 >> branch_weekend >> [empty_task_3, empty_task_4]
    # [END howto_operator_day_of_week_branch]
