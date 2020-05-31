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

"""Example DAG demonstrating the usage of the XComArgs."""
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.decorators import apply_defaults


class CustomOp(DummyOperator):
    template_fields = ("field",)

    @apply_defaults
    def __init__(self, field=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.field = field


args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}


def dummy(*args, **kwargs):
    """Dummy function"""
    return "pass"


with DAG(dag_id='xcomargs_test_1', default_args={"start_date": datetime.today()}) as dag1:
    op1 = DummyOperator(task_id="op1")
    op2 = CustomOp(task_id="op2", field=op1.output)
    op3 = CustomOp(task_id="op3")
    op3.field = op2.output


with DAG(dag_id='xcomargs_test_2', default_args={"start_date": datetime.today()}) as dag2:
    op1 = DummyOperator(task_id="op1")
    op2 = CustomOp(task_id="op2", field=op1.output)
    op3 = CustomOp(task_id="op3")
    op3.field = op2.output
