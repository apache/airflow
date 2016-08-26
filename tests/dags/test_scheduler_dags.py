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

from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
DEFAULT_DATE = datetime(2100, 1, 1)

# DAG tests backfill with pooled tasks
# Previously backfill would queue the task but never run it
dag1 = DAG(
    dag_id='test_start_date_scheduling',
    start_date=datetime(2100, 1, 1))
dag1_task1 = DummyOperator(
    task_id='dummy',
    dag=dag1,
    owner='airflow')
