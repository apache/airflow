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
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker_swarm import DockerSwarmOperator

dag = DAG(
    'docker_swarm_sample',
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

with dag as dag:
    t1 = DockerSwarmOperator(
        api_version='auto',
        docker_url='tcp://localhost:2375',  # Set your docker URL
        command='/bin/sleep 10',
        image='centos:latest',
        auto_remove=True,
        task_id='sleep_with_swarm',
    )
