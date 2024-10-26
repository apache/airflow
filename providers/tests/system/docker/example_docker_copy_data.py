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
"""
This sample "listen to directory". move the new file and print it,
using docker-containers.
The following operators are being used: DockerOperator,
BashOperator & ShortCircuitOperator.
TODO: Review the workflow, change it accordingly to
your environment & enable the code.
"""

from __future__ import annotations

import os
from datetime import datetime

from docker.types import Mount

from airflow import models
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "docker_sample_copy_data"

with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "docker"],
) as dag:
    locate_file_cmd = """
        sleep 10
        find {{params.source_location}} -type f  -printf "%f\n" | head -1
    """

    t_view = BashOperator(
        task_id="view_file",
        bash_command=locate_file_cmd,
        do_xcom_push=True,
        params={"source_location": "/your/input_dir/path"},
        dag=dag,
    )

    t_is_data_available = ShortCircuitOperator(
        task_id="check_if_data_available",
        python_callable=lambda task_output: not task_output == "",
        op_kwargs=dict(task_output=t_view.output),
        dag=dag,
    )

    t_move = DockerOperator(
        api_version="1.19",
        docker_url="tcp://localhost:2375",  # replace it with swarm/docker endpoint
        image="centos:latest",
        network_mode="bridge",
        mounts=[
            Mount(source="/your/host/input_dir/path", target="/your/input_dir/path", type="bind"),
            Mount(source="/your/host/output_dir/path", target="/your/output_dir/path", type="bind"),
        ],
        command=[
            "/bin/bash",
            "-c",
            "/bin/sleep 30; "
            f"/bin/mv {{{{ params.source_location }}}}/{t_view.output} {{{{ params.target_location }}}};"
            f"/bin/echo '{{{{ params.target_location }}}}/{t_view.output}';",
        ],
        task_id="move_data",
        do_xcom_push=True,
        params={"source_location": "/your/input_dir/path", "target_location": "/your/output_dir/path"},
        dag=dag,
    )

    t_print = DockerOperator(
        api_version="1.19",
        docker_url="tcp://localhost:2375",
        image="centos:latest",
        mounts=[Mount(source="/your/host/output_dir/path", target="/your/output_dir/path", type="bind")],
        command=f"cat {t_move.output}",
        task_id="print",
        dag=dag,
    )

    (
        # TEST BODY
        t_is_data_available >> t_move >> t_print
    )

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
