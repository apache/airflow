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
This is an example dag for ECSOperator.

The task "hello_world" runs `hello-world` task in `c` cluster.
It overrides the command in the `hello-world-container` container.
"""

import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import ECSOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.datetime(2020, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="ecs_fargate_dag",
    default_args=DEFAULT_ARGS,
    default_view="graph",
    schedule_interval=None,
    tags=["example"],
)
# generate dag documentation
dag.doc_md = __doc__

# [START howto_operator_ecs]
hello_world = ECSOperator(
    task_id="hello_world",
    dag=dag,
    aws_conn_id="aws_default",
    cluster="c",
    task_definition="hello-world",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "hello-world-container",
                "command": ["echo", "hello", "world"],
            },
        ],
    },
    network_configuration={
        "awsvpcConfiguration": {
            "securityGroups": ["sg-123abc"],
            "subnets": ["subnet-123456ab"],
        },
    },
    tags={
        "Customer": "X",
        "Project": "Y",
        "Application": "Z",
        "Version": "0.0.1",
        "Environment": "Development",
    },
    awslogs_group="/ecs_logs/group_a",
    awslogs_stream_prefix="prefix_b",
)
# [END howto_operator_ecs]
