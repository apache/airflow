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
Example DAG demonstrating the usage of BranchPythonOperator with depends_on_past=True, where tasks may be run
or skipped on alternating runs.
"""
import datetime

from airflow import DAG
from airflow.operators.datetime_branch import DateTimeBranchOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
    "owner": "airflow",
}

dag = DAG(
    dag_id="example_datetime_branch_operator",
    start_date=days_ago(2),
    default_args=args,
    tags=["example"],
    schedule_interval="@daily",
)

# [START howto_operator_datetime_branch]
dummy_task_1 = DummyOperator(task_id='date_in_range', dag=dag)
dummy_task_2 = DummyOperator(task_id='date_outside_range', dag=dag)

cond1 = DateTimeBranchOperator(
    task_id='datetime_branch',
    follow_task_ids_if_true=['date_in_range'],
    follow_task_ids_if_false=['date_outside_range'],
    target_upper=datetime.datetime(2020, 10, 10, 15, 0, 0),
    target_lower=datetime.datetime(2020, 10, 10, 14, 0, 0),
    dag=dag,
)

# Run dummy_task_1 if cond1 executes between 2020-10-10 14:00:00 and 2020-10-10 15:00:00
cond1 >> [dummy_task_1, dummy_task_2]
# [END howto_operator_datetime_branch]

# [START howto_operator_datetime_branch_next_day]
cond2 = DateTimeBranchOperator(
    task_id='datetime_branch',
    follow_task_ids_if_true=['date_in_range'],
    follow_task_ids_if_false=['date_outside_range'],
    target_upper=datetime.time(0, 0, 0),
    target_lower=datetime.time(15, 0, 0),
    dag=dag,
)

# Since target_lower happens after target_upper, target_upper will be moved to the following day
# Run dummy_task_1 if cond2 executes between 15:00:00 and 00:00:00 of the following day
cond2 >> [dummy_task_1, dummy_task_2]
# [END howto_operator_datetime_branch_next_day]
