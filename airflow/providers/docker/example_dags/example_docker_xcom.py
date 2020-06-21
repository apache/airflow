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
# pylint: disable=missing-function-docstring
"""
This sample writes and pull from xcom using DockerOperator.
The following operators are being used: DockerOperator &
BashOperator.
"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="example_docker_xcom",
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=2,
    default_args={
        "owner": "airflow",
        "start_date": days_ago(5),
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    write_xcom_last = DockerOperator(
        task_id="write_xcom_last",
        image="python:3-slim",
        api_version="auto",
        command="""python -c "import logging;[
            print(i) for i in range(10)];logging.warning('this is a warning')" """,
        do_xcom_push=True,
        xcom_all=False,
        auto_remove=True,
        network_mode="bridge",
    )

    write_xcom_all = DockerOperator(
        task_id="write_xcom_all",
        image="python:3-slim",
        api_version="auto",
        command="""python -c "import logging;[
            print(i) for i in range(10)];logging.warning('this is a warning')" """,
        do_xcom_push=True,
        xcom_all=True,
        auto_remove=True,
        network_mode="bridge",
    )

    pull_xcom_last = DockerOperator(
        task_id="pull_xcom",
        image="ubuntu:20.04",
        api_version="auto",
        command="""echo {{ ti.xcom_pull(task_ids="write_xcom_last") }}""",
        do_xcom_push=True,
        xcom_all=False,
        auto_remove=True,
        network_mode="bridge",
    )

    pull_xcom_all = BashOperator(
        task_id="pull_xcom_all",
        bash_command="""echo '{{ ti.xcom_pull(task_ids="write_xcom_all") }}' """,
        do_xcom_push=True,
    )

    write_xcom_last >> pull_xcom_last
    write_xcom_all >> pull_xcom_all
