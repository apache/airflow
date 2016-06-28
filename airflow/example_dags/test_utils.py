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
"""Used for unit tests"""
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from datetime import datetime

dag = DAG(
    dag_id='test_utils',
    schedule_interval=None,
)

task = BashOperator(
    task_id='sleeps_forever',
    dag=dag,
    bash_command="sleep 10000000000",
    start_date=datetime(2016, 1, 1),
    owner='airflow')
