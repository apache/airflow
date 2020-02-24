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

"""Example DAG demonstrating the DummyOperator and a custom DummySkipOperator which skips by default."""

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}


# Create some placeholder operators
class DummySkipOperator(DummyOperator):
    """Dummy operator which always skips the task."""

    ui_color = '#e8b7e4'

    def execute(self, context):
        raise AirflowSkipException


def create_test_pipeline(suffix, trigger_rule, dag_):
    """
    Instantiate a number of operators for the given DAG.

    :param str suffix: Suffix to append to the operator task_ids
    :param str trigger_rule: TriggerRule for the join task
    :param DAG dag_: The DAG to run the operators on
    """
    skip_operator = DummySkipOperator(task_id='skip_operator_{}'.format(suffix), dag=dag_)
    always_true = DummyOperator(task_id='always_true_{}'.format(suffix), dag=dag_)
    join = DummyOperator(task_id=trigger_rule, dag=dag_, trigger_rule=trigger_rule)
    final = DummyOperator(task_id='final_{}'.format(suffix), dag=dag_)

    skip_operator >> join
    always_true >> join
    join >> final


dag = DAG(dag_id='example_skip_dag', default_args=args, tags=['example'])
create_test_pipeline('1', 'all_success', dag)
create_test_pipeline('2', 'one_success', dag)
