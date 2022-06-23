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
import os
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsOperator

with DAG(
    dag_id='example_ecs',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # [START howto_operator_ecs]
    hello_world = EcsOperator(
        task_id="hello_world",
        cluster=os.environ.get("CLUSTER_NAME", "existing_cluster_name"),
        task_definition=os.environ.get("TASK_DEFINITION", "existing_task_definition_name"),
        launch_type="EXTERNAL|EC2",
        aws_conn_id="aws_ecs",
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
        #    [START howto_awslogs_ecs]
        awslogs_group="/ecs/hello-world",
        awslogs_region="aws-region",
        awslogs_stream_prefix="ecs/hello-world-container"
        #   [END howto_awslogs_ecs]
    )
    # [END howto_operator_ecs]
