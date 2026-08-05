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
This example illustrates three DAGs. One

The parent DAG (ray_dynamic_config_upstream_dag) uses TriggerDagRunOperator to trigger the other two:
* ray_dynamic_config_downstream_dag_1
* ray_dynamic_config_downstream_dag_2

Each downstream DAG retrieves the context data (run_context) from dag_run.conf, which is passed by the parent DAG.

The print_context tasks in the downstream DAGs output the received context to the logs.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml
from jinja2 import Template
from pendulum import datetime

from airflow.providers.ray.decorators.ray import ray
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, task

CONN_ID = "ray_conn"
RAY_SPEC = Path(__file__).parent / "scripts/ray.yaml"
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


def slugify(value):
    """
    Replace invalid characters with hyphens and make lowercase.
    """
    return re.sub(r"[^\w\-\.]", "-", value).lower()


def create_config_from_context(context, **kwargs):
    default_name = "{{ dag.dag_id }}-{{ dag_run.id }}"

    raycluster_name_template = context.get("dag_run").conf.get("raycluster_name", default_name)
    raycluster_name = Template(raycluster_name_template).render(context).replace("_", "-")
    raycluster_name = slugify(raycluster_name)

    raycluster_k8s_yml_filename_template = context.get("dag_run").conf.get(
        "raycluster_k8s_yml_filename", default_name + ".yml"
    )
    raycluster_k8s_yml_filename = (
        Template(raycluster_k8s_yml_filename_template).render(context).replace("_", "-")
    )
    raycluster_k8s_yml_filename = slugify(raycluster_k8s_yml_filename)

    with open(RAY_SPEC) as file:
        data = yaml.safe_load(file)
        data["metadata"]["name"] = raycluster_name

    NEW_RAY_K8S_SPEC = Path(__file__).parent / "scripts" / raycluster_k8s_yml_filename
    with open(NEW_RAY_K8S_SPEC, "w") as file:
        yaml.safe_dump(data, file, default_flow_style=False)

    config = dict(RAY_TASK_CONFIG)
    config["ray_cluster_yaml"] = str(NEW_RAY_K8S_SPEC)
    return config


def print_context(**kwargs):
    # Retrieve `conf` passed from the parent DAG
    print(kwargs)
    cluster_name = kwargs.get("dag_run").conf.get("raycluster_name", "No ray cluster name provided")
    raycluster_k8s_yml_filename = kwargs.get("dag_run").conf.get(
        "raycluster_k8s_yml_filename", "No ray cluster YML filename provided"
    )
    print(f"Received cluster name: {cluster_name}")
    print(f"Received cluster K8s YML filename: {raycluster_k8s_yml_filename}")


# Downstream 1
with DAG(
    dag_id="ray_dynamic_config_child_1",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:
    print_context_task = PythonOperator(
        task_id="print_context",
        python_callable=print_context,
    )
    print_context_task

    @task
    def generate_data():
        return [1, 2, 3]

    @ray.task(config=create_config_from_context)
    def process_data_with_ray(data):
        import numpy as np
        import ray

        @ray.remote
        def cubic(x):
            return x**3

        ray.init()
        data = np.array(data)
        futures = [cubic.remote(x) for x in data]
        results = ray.get(futures)
        mean = np.mean(results)
        print(f"Mean of this population is {mean}")
        return mean

    data = generate_data()
    process_data_with_ray(data)


# Downstream 2
with DAG(
    dag_id="ray_dynamic_config_child_2",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:
    print_context_task = PythonOperator(
        task_id="print_context",
        python_callable=print_context,
    )

    @task
    def generate_data():
        return [1, 2, 3]

    @ray.task(config=create_config_from_context)
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


# Upstream
with DAG(
    dag_id="ray_dynamic_config_parent",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:
    empty_task = EmptyOperator(task_id="empty_task")

    trigger_dag_1 = TriggerDagRunOperator(
        task_id="trigger_downstream_dag_1",
        trigger_dag_id="ray_dynamic_config_child_1",
        conf={
            "raycluster_name": "first-{{ dag_run.id }}",
            "raycluster_k8s_yml_filename": "first-{{ dag_run.id }}.yaml",
        },
    )

    trigger_dag_2 = TriggerDagRunOperator(
        task_id="trigger_downstream_dag_2",
        trigger_dag_id="ray_dynamic_config_child_2",
        conf={},
    )

    # Illustrates that by default two DAG runs of the same DAG will be using different Ray clusters
    # Disabled because in the local dev MacOS we're only managing to spin up two Ray Cluster services concurrently
    # trigger_dag_3 = TriggerDagRunOperator(
    #    task_id="trigger_downstream_dag_3",
    #    trigger_dag_id="ray_dynamic_config_child_2",
    #    conf={},
    # )

    empty_task >> trigger_dag_1
    trigger_dag_1 >> trigger_dag_2
    # trigger_dag_1 >> trigger_dag_3
