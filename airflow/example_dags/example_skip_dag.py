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
"""Example DAG demonstrating the EmptyOperator and a custom EmptySkipOperator which skips by default."""
from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule


# Create some placeholder operators
class EmptySkipOperator(EmptyOperator):
    """Empty operator which always skips the task."""

    ui_color = '#e8b7e4'

    def execute(self, context: Context):
        raise AirflowSkipException


def create_test_pipeline(suffix, trigger_rule):
    """
    Instantiate a number of operators for the given DAG.

    :param str suffix: Suffix to append to the operator task_ids
    :param str trigger_rule: TriggerRule for the join task
    :param DAG dag_: The DAG to run the operators on
    """
    skip_operator = EmptySkipOperator(task_id=f'skip_operator_{suffix}')
    always_true = EmptyOperator(task_id=f'always_true_{suffix}')
    join = EmptyOperator(task_id=trigger_rule, trigger_rule=trigger_rule)
    final = EmptyOperator(task_id=f'final_{suffix}')

    skip_operator >> join
    always_true >> join
    join >> final


with DAG(
    dag_id='example_skip_dag',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
) as dag:
    create_test_pipeline('1', TriggerRule.ALL_SUCCESS)
    create_test_pipeline('2', TriggerRule.ONE_SUCCESS)
