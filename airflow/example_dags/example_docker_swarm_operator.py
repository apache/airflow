# -*- coding: utf-8 -*-
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
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.contrib.operators.docker_swarm_operator import DockerSwarmOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'docker_swarm_sample',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False
)

with dag as dag:
    t1 = DockerSwarmOperator(
        api_version='auto',
        docker_url='tcp://localhost:2375', # Set your docker URL
        command='/bin/sleep 10',
        image='centos:latest',
        auto_remove=True,
        task_id='sleep_with_swarm',
    )
"""
