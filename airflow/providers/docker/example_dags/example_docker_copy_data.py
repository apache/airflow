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

from datetime import datetime, timedelta

from docker.types import Mount

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.docker.operators.docker import DockerOperator

dag = DAG(
    "docker_sample_copy_data",
    default_args={"retries": 1},
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

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
        "/bin/mv {{ params.source_location }}/" + str(t_view.output) + " {{ params.target_location }};"
        "/bin/echo '{{ params.target_location }}/" + f"{t_view.output}';",
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

t_is_data_available.set_downstream(t_move)
t_move.set_downstream(t_print)

# Task dependencies created via `XComArgs`:
#   t_view >> t_is_data_available
