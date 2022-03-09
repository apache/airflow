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
- You must have the `c` cluster setup with EC2 and/or EXTERNAL instances.
- You must ensure that the CloudWatch permissions are updated
It overrides the command in the `hello-world-container` container.
"""

import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsOperator

dag = DAG(
    dag_id="ecs_ec2_dag",
    default_view="graph",
    schedule_interval=None,
    start_date=datetime.datetime(2020, 1, 1),
    catchup=False,
    tags=["example"],
)
# generate dag documentation
dag.doc_md = __doc__

# [START howto_operator_ecs]
hello_world = EcsOperator(
    task_id="hello_world",
    dag=dag,
    cluster="test-hybrid",
    task_definition="test",
    launch_type="EXTERNAL",
    overrides={
        "containerOverrides": [
            {
                "name": "hello-world-container",
                "command": ["echo", "hello", "world"],
            },
        ],
    },
    tags={
        "Customer": "X",
        "Project": "Y",
        "Application": "Z",
        "Version": "0.0.1",
        "Environment": "Development",
    },
    awslogs_group="/ecs/hello-world",
    awslogs_stream_prefix="ecs",  # prefix with container name
)
# [END howto_operator_ecs]
