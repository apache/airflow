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

import os
from datetime import datetime

from airflow import models
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "docker_test"

with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "docker"],
) as dag:
    t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)
    t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)
    # [START howto_operator_docker]
    t3 = DockerOperator(
        docker_url="unix://var/run/docker.sock",  # Set your docker URL
        command="/bin/sleep 30",
        image="centos:latest",
        network_mode="bridge",
        task_id="docker_op_tester",
        dag=dag,
    )
    # [END howto_operator_docker]

    t4 = BashOperator(task_id="print_hello", bash_command='echo "hello world!!!"', dag=dag)
    # t1 >> t2
    # t1 >> t3
    # t3 >> t4

    (
        # TEST BODY
        t1 >> [t2, t3] >> t4
    )

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
