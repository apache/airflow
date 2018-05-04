# -*- coding: utf-8 -*-
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

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from textwrap import dedent


DEFAULT_DATE = datetime(2016, 1, 1)

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
}

dag = DAG(dag_id='test_impersonation', default_args=args)

run_as_user = 'airflow_test_user'

test_command = dedent(
    """\
    if [ '{user}' != "$(whoami)" ]; then
        echo current user is not {user}!
        exit 1
    fi
    """.format(user=run_as_user))

task = BashOperator(
    task_id='test_impersonated_user',
    bash_command=test_command,
    dag=dag,
    run_as_user=run_as_user,
)
