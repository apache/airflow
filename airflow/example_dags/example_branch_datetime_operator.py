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
Example DAG demonstrating the usage of DateTimeBranchOperator with datetime as well as time objects as
targets.
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.datetime import BranchDateTimeOperator

dag1 = DAG(
    dag_id="example_branch_datetime_operator",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    schedule="@daily",
)

# [START howto_branch_datetime_operator]
empty_task_11 = EmptyOperator(task_id="date_in_range", dag=dag1)
empty_task_21 = EmptyOperator(task_id="date_outside_range", dag=dag1)

cond1 = BranchDateTimeOperator(
    task_id="datetime_branch",
    follow_task_ids_if_true=["date_in_range"],
    follow_task_ids_if_false=["date_outside_range"],
    target_upper=pendulum.datetime(2020, 10, 10, 15, 0, 0),
    target_lower=pendulum.datetime(2020, 10, 10, 14, 0, 0),
    dag=dag1,
)

# Run empty_task_11 if cond1 executes between 2020-10-10 14:00:00 and 2020-10-10 15:00:00
cond1 >> [empty_task_11, empty_task_21]
# [END howto_branch_datetime_operator]


dag2 = DAG(
    dag_id="example_branch_datetime_operator_2",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    schedule="@daily",
)
# [START howto_branch_datetime_operator_next_day]
empty_task_12 = EmptyOperator(task_id="date_in_range", dag=dag2)
empty_task_22 = EmptyOperator(task_id="date_outside_range", dag=dag2)

cond2 = BranchDateTimeOperator(
    task_id="datetime_branch",
    follow_task_ids_if_true=["date_in_range"],
    follow_task_ids_if_false=["date_outside_range"],
    target_upper=pendulum.time(0, 0, 0),
    target_lower=pendulum.time(15, 0, 0),
    dag=dag2,
)

# Since target_lower happens after target_upper, target_upper will be moved to the following day
# Run empty_task_12 if cond2 executes between 15:00:00, and 00:00:00 of the following day
cond2 >> [empty_task_12, empty_task_22]
# [END howto_branch_datetime_operator_next_day]

dag3 = DAG(
    dag_id="example_branch_datetime_operator_3",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    schedule="@daily",
)
# [START howto_branch_datetime_operator_logical_date]
empty_task_13 = EmptyOperator(task_id="date_in_range", dag=dag3)
empty_task_23 = EmptyOperator(task_id="date_outside_range", dag=dag3)

cond3 = BranchDateTimeOperator(
    task_id="datetime_branch",
    use_task_logical_date=True,
    follow_task_ids_if_true=["date_in_range"],
    follow_task_ids_if_false=["date_outside_range"],
    target_upper=pendulum.datetime(2020, 10, 10, 15, 0, 0),
    target_lower=pendulum.datetime(2020, 10, 10, 14, 0, 0),
    dag=dag3,
)

# Run empty_task_13 if cond3 executes between 2020-10-10 14:00:00 and 2020-10-10 15:00:00
cond3 >> [empty_task_13, empty_task_23]
# [END howto_branch_datetime_operator_logical_date]
