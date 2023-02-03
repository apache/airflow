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
from __future__ import annotations

import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="test_failing_bash_operator",
    default_args={"owner": "airflow", "retries": 3, "start_date": datetime.datetime(2022, 1, 1)},
    schedule="0 0 * * *",
    dagrun_timeout=datetime.timedelta(minutes=60),
)

task = BashOperator(task_id="failing_task", bash_command="sleep 1 && exit 1", dag=dag)

if __name__ == "__main__":
    dag.cli()
