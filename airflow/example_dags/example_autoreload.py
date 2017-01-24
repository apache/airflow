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
from __future__ import print_function

import json
import os
from datetime import datetime, timedelta

from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator

config_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'config'
)

# these files will be watched for changes
__resource_file_selectors__ = [
    (config_path, ('.json'))
]

with open(os.path.join(config_path, 'autoreload.json')) as config_data:
    config_object = json.load(config_data)

# instances of MaterializedVariable will be watched for changes and
# trigger autoreload of the DAG
number_of_days_back_var = Variable.materialize(
    'number_of_days_back',
    default_value=7
)

number_of_days_back = datetime.combine(
    datetime.today() -
    timedelta(int(number_of_days_back_var.val)), datetime.min.time()
)

args = {
    'owner': config_object['dag_owner'],
    'start_date': number_of_days_back,
}

dag = DAG(
    dag_id='example_autoreload',
    default_args=args,
    schedule_interval=config_object['schedule_interval']
)

run_this = DummyOperator(
    task_id='dummy_operator',
    dag=dag)
