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
Example DAG demonstrating a workflow with nested branching. The join tasks are created with
``none_failed_or_skipped`` trigger rule such that they are skipped whenever their corresponding
``BranchPythonOperator`` are skipped.
"""

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago

with DAG(dag_id="example_nested_branch_dag", start_date=days_ago(2), schedule_interval="@daily") as dag:
    branch_1 = BranchPythonOperator(task_id="branch_1", python_callable=lambda: "true_1")
    join_1 = DummyOperator(task_id="join_1", trigger_rule="none_failed_or_skipped")
    true_1 = DummyOperator(task_id="true_1")
    false_1 = DummyOperator(task_id="false_1")
    branch_2 = BranchPythonOperator(task_id="branch_2", python_callable=lambda: "true_2")
    join_2 = DummyOperator(task_id="join_2", trigger_rule="none_failed_or_skipped")
    true_2 = DummyOperator(task_id="true_2")
    false_2 = DummyOperator(task_id="false_2")
    false_3 = DummyOperator(task_id="false_3")

    branch_1 >> true_1 >> join_1
    branch_1 >> false_1 >> branch_2 >> [true_2, false_2] >> join_2 >> false_3 >> join_1
