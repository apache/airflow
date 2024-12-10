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
from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk.definitions.dag import dag

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


@dag()
def basic_templated_dag():
    BashOperator(
        task_id="task1",
        bash_command="echo 'Logical date is {{ logical_date }}'",
    )


basic_templated_dag()
