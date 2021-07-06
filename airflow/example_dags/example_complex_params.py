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

"""Example DAG demonstrating the usage of the complex params."""

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    "example_complex_params",
    params={
        'int_param': Param(10, type="integer", minimum=0, maximum=20),  # non default int param
        'str_param': Param(type="string", minLength=2, maxLength=4),  # a mandatory str param
        'old_param': 'old_way_of_passing',
        'simple_param': Param('im_just_like_old_param'),  # i.e. no type checking
        'email_param': Param(
            'example@example.com', type='string', format='idn-email', minLength=5, maxLength=255
        ),
    },
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    all_params = BashOperator(
        task_id='all_param',
        bash_command="echo {{ params.int_param }} {{ params.str_param }} {{ params.old_param }} "
        "{{ params.simple_param }} {{ params.email_param }} {{ params.task_param }}",
        params={'task_param': Param('im_a_task_param', type='string')},
    )
