##
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

from airflow.models.dag import DAG
from airflow.providers.standard.core.operators.python import PythonOperator

dag = DAG(
    dag_id="test_dag_xcom_openlineage",
    default_args={"owner": "airflow", "retries": 3, "start_date": datetime.datetime(2022, 1, 1)},
    schedule="0 0 * * *",
    dagrun_timeout=datetime.timedelta(minutes=60),
)


def push_and_pull(ti, **kwargs):
    ti.xcom_push(key="pushed_key", value="asdf")
    ti.xcom_pull(key="pushed_key")


task = PythonOperator(task_id="push_and_pull", python_callable=push_and_pull, dag=dag)

if __name__ == "__main__":
    dag.cli()
