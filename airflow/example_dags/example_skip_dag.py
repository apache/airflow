# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.exceptions import AirflowSkipException

seven_days_ago = datetime.combine(datetime.today() - timedelta(1),
                                  datetime.min.time())
args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}


# Create some placeholder operators
class DummySkipOperator(DummyOperator):
    ui_color = '#e8b7e4'

    def execute(self, context):
        raise AirflowSkipException


dag = DAG(dag_id='example_skip_dag', default_args=args)


def create_test_pipeline(suffix, trigger_rule, dag):

    skip_operator = DummySkipOperator(task_id='skip_operator_{}'.format(suffix), dag=dag)

    always_true = DummyOperator(task_id='always_true_{}'.format(suffix), dag=dag)

    join = DummyOperator(task_id=trigger_rule, dag=dag, trigger_rule=trigger_rule)

    join.set_upstream(skip_operator)
    join.set_upstream(always_true)

    final = DummyOperator(task_id='final_{}'.format(suffix), dag=dag)
    final.set_upstream(join)


create_test_pipeline('1', 'all_success', dag)
create_test_pipeline('2', 'one_success', dag)


