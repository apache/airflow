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

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from textwrap import dedent


DEFAULT_DATE = datetime(2016, 1, 1)

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
}

dag = DAG(dag_id='test_no_impersonation', default_args=args)

test_command = dedent(
    """\
    sudo ls
    if [ $? -ne 0 ]; then
        echo 'current uid does not have root privileges!'
        exit 1
    fi
    """)

task = BashOperator(
    task_id='test_superuser',
    bash_command=test_command,
    dag=dag,
)
