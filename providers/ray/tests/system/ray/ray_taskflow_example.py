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
from pathlib import Path

from pendulum import datetime

from airflow.providers.ray.decorators.ray import ray
from airflow.sdk import dag, task

CONN_ID = "ray_conn"
RAY_SPEC_FILENAME = os.getenv("RAY_SPEC_FILENAME", "ray.yaml")
RAY_SPEC = Path(__file__).parent / "scripts" / RAY_SPEC_FILENAME

FOLDER_PATH = Path(__file__).parent / "ray_scripts"
RAY_TASK_CONFIG = {
    "conn_id": CONN_ID,
    "runtime_env": {"working_dir": str(FOLDER_PATH), "pip": ["numpy"]},
    "num_cpus": 1,
    "num_gpus": 0,
    "memory": 0,
    "poll_interval": 5,
    "ray_cluster_yaml": str(RAY_SPEC),
    "xcom_task_key": "dashboard",
}


@dag(
    dag_id="Ray_Taskflow_Example",
    start_date=datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ray", "example"],
)
def ray_taskflow_dag():

    @task
    def generate_data():
        return [1, 2, 3]

    @ray.task(config=RAY_TASK_CONFIG)
    def process_data_with_ray(data):
        import numpy as np
        import ray

        @ray.remote
        def square(x):
            return x**2

        ray.init()
        data = np.array(data)
        futures = [square.remote(x) for x in data]
        results = ray.get(futures)
        mean = np.mean(results)
        print(f"Mean of this population is {mean}")
        return mean

    data = generate_data()
    process_data_with_ray(data)


ray_example_dag = ray_taskflow_dag()
