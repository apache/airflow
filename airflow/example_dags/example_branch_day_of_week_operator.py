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

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.dates import days_ago

args = {
    "owner": "airflow",
}

with DAG(
    dag_id="example_weekday_branch_operator",
    start_date=days_ago(2),
    default_args=args,
    tags=["example"],
    schedule_interval="@daily",
) as dag:

    # [START howto_operator_day_of_week_branch]
    dummy_task_1 = DummyOperator(task_id='branch_true', dag=dag)
    dummy_task_2 = DummyOperator(task_id='branch_false', dag=dag)

    branch = BranchDayOfWeekOperator(
        task_id="make_choice",
        follow_task_ids_if_true="branch_true",
        follow_task_ids_if_false="branch_false",
        week_day="Monday",
    )

    # Run dummy_task_1 if branch executes on Monday
    branch >> [dummy_task_1, dummy_task_2]
    # [END howto_operator_day_of_week_branch]
