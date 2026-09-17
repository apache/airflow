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
Example Dag for :class:`~airflow.providers.dbt.core.operators.dbt.DbtKubernetesRunOperator`.

A two-phase hourly pipeline that demonstrates the common real-world patterns:

* **Pre-processing phase** — seeds and view materialisations that run at the
  top of every hour before incremental models can read from them.  The git
  repo is cached to S3 after the first successful clone, so subsequent runs
  recover if git is temporarily unreachable.
* **Hourly phase** — waits for an offset (in case the hour boundary is noisy),
  then polls for upstream data readiness before running the incremental models.

Other patterns shown:
* :class:`~airflow.sdk.TaskGroup` to organise the two phases.
* An ``init_container`` that side-loads the AWS CLI into a shared
  ``emptyDir`` volume so the main container can upload ``target/`` to S3
  without the CLI being baked into the runner image.
* Using ``git_conn_id`` to supply the clone token instead of putting it in
  ``env_vars`` directly.
* Using ``warehouse_conn_id`` so warehouse credentials are resolved from an
  Airflow connection rather than being injected as raw env vars.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from kubernetes.client import models as k8s

from airflow import DAG
from airflow.providers.dbt.core.operators.dbt import DbtKubernetesRunOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import TaskGroup

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "dev")
DAG_ID = "example_dbt_kubernetes"

# Runner image: needs bash, git, and curl/unzip so the init container can
# download the AWS CLI.  dbt itself is installed at run time by the
# DbtKubernetesRunOperator entrypoint via `command_prefix="uv run"`.
DBT_IMAGE = os.environ.get("DBT_IMAGE", "ghcr.io/astral-sh/uv:python3.12-bookworm")

# Connection IDs expected to exist in the Airflow metadata database.
K8S_CONN_ID = os.environ.get("K8S_CONN_ID", "kubernetes_default")
GIT_CONN_ID = os.environ.get("GIT_CONN_ID", "git_default")
WAREHOUSE_CONN_ID = os.environ.get("WAREHOUSE_CONN_ID", "warehouse_default")
S3_CONN_ID = os.environ.get("S3_CONN_ID", "aws_default")

GIT_REPO_URL = os.environ.get("DBT_GIT_REPO_URL", "github.com/acme/dbt-project.git")
GIT_BRANCH = os.environ.get("DBT_GIT_BRANCH", "main")
K8S_NAMESPACE = os.environ.get("K8S_NAMESPACE", "airflow")

ARTIFACT_BASE = f"s3://my-dbt-artifacts/{ENV_ID}"
GIT_CACHE_BASE = f"s3://my-dbt-artifacts/git-cache/{ENV_ID}"

# ---------------------------------------------------------------------------
# AWS CLI init container
# ---------------------------------------------------------------------------
# The runner image carries python + uv + git but NOT the AWS CLI.  An
# init_container downloads and installs it into a shared emptyDir volume
# (/aws-cli) that the main container mounts on PATH.  This avoids baking the
# CLI into the image while keeping the install transparent to the operator.
# ---------------------------------------------------------------------------
_AWSCLI_VOLUME = k8s.V1Volume(name="aws-cli", empty_dir=k8s.V1EmptyDirVolumeSource())
_AWSCLI_MOUNT = k8s.V1VolumeMount(name="aws-cli", mount_path="/aws-cli")
_AWSCLI_INIT_CONTAINER = k8s.V1Container(
    name="install-awscli",
    image=DBT_IMAGE,
    command=[
        "bash",
        "-c",
        "apt-get install -y -q --no-install-recommends curl unzip && "
        "ARCH=$(uname -m) && "
        'curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip" -o /tmp/awscliv2.zip && '
        "unzip -q /tmp/awscliv2.zip -d /tmp/ && "
        "/tmp/aws/install --bin-dir /aws-cli/bin --install-dir /aws-cli/lib",
    ],
    volume_mounts=[_AWSCLI_MOUNT],
)


def _check_data_readiness(**kwargs) -> bool:
    """Return ``True`` once the upstream source data for the current hour is available.

    Replace this with your own readiness check — for example a Spark/Trino
    query, an S3 key-existence check, or an API call.  The PythonSensor retries
    every ``poke_interval`` seconds until this returns ``True`` or ``timeout``
    is reached.
    """
    return True


# Shared KubernetesPodOperator kwargs that are the same for every task.
_K8S_KWARGS = dict(
    image=DBT_IMAGE,
    namespace=K8S_NAMESPACE,
    kubernetes_conn_id=K8S_CONN_ID,
    command_prefix="uv run",
    git_repo_url=GIT_REPO_URL,
    git_branch=GIT_BRANCH,
    git_conn_id=GIT_CONN_ID,
    warehouse_conn_id=WAREHOUSE_CONN_ID,
    artifact_dest=ARTIFACT_BASE,
    artifact_conn_id=S3_CONN_ID,
    init_containers=[_AWSCLI_INIT_CONTAINER],
    volumes=[_AWSCLI_VOLUME],
    volume_mounts=[_AWSCLI_MOUNT],
)

with DAG(
    dag_id=DAG_ID,
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["example", "dbt", "kubernetes"],
    default_args={"retries": 2},
) as dag:
    start = EmptyOperator(task_id="start")

    # [START howto_operator_dbt_kubernetes_pre_processing]
    with TaskGroup(group_id="pre_processing") as pre_processing:
        # Seeds and view models run before the incremental models in the
        # hourly phase can reference them.  git_cache_dest keeps the clone
        # available even if the git server is temporarily unreachable.
        pre_processing_dbt = DbtKubernetesRunOperator(
            task_id="dbt_seeds_and_views",
            steps=[
                "dbt debug --profile airflow --target dev",
                "dbt parse",
                (
                    "dbt build"
                    " --select 'config.materialized:view,path:seeds resource_type:seed'"
                    ' --vars \'{"data_interval_start": "{{ data_interval_start }}"}\''
                    " --profile airflow --target dev"
                ),
            ],
            git_cache_dest=GIT_CACHE_BASE,
            **_K8S_KWARGS,
        )
    # [END howto_operator_dbt_kubernetes_pre_processing]

    # [START howto_operator_dbt_kubernetes_hourly]
    with TaskGroup(group_id="hourly_processing") as hourly_processing:
        # Wait a small offset after the hour boundary before trying to read
        # upstream data (common when source pipelines land a few minutes late).
        wait_for_offset = TimeDeltaSensor(
            task_id="wait_for_offset",
            delta=timedelta(minutes=1),
            deferrable=False,
        )

        # Poll until the upstream source data for this hour has fully landed.
        await_data = PythonSensor(
            task_id="await_data",
            python_callable=_check_data_readiness,
            mode="poke",
            poke_interval=3 * 60,
            timeout=5 * 60,
        )

        hourly_dbt = DbtKubernetesRunOperator(
            task_id="dbt_incremental_models",
            steps=[
                "dbt debug --profile airflow --target dev",
                "dbt parse",
                (
                    "dbt build"
                    " --select tag:hourly"
                    ' --vars \'{"data_interval_start": "{{ data_interval_start }}"}\''
                    " --profile airflow --target dev"
                ),
            ],
            **_K8S_KWARGS,
        )

        wait_for_offset >> await_data >> hourly_dbt
    # [END howto_operator_dbt_kubernetes_hourly]

    end = EmptyOperator(task_id="end")

    start >> [pre_processing, hourly_processing] >> end


# ---------------------------------------------------------------------------
# Standard Airflow system-test tail. Guarded so the module still imports (and
# the Dag still parses) in environments without the system-test harness.
# ---------------------------------------------------------------------------
try:
    from tests_common.test_utils.system_tests import get_test_run  # type: ignore
except ImportError:  # pragma: no cover - harness not installed
    get_test_run = None

if get_test_run is not None:
    # Needed to run the example Dag with pytest (see providers docs on system tests).
    test_run = get_test_run(dag)
