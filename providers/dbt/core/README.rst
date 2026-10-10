 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

``apache-airflow-providers-dbt-core``
=====================================

Run `dbt Core <https://docs.getdbt.com/docs/core/installation-overview>`__ jobs
on Kubernetes from Apache Airflow. The provider ships a single operator,
:class:`~airflow.providers.dbt.core.operators.dbt.DbtKubernetesRunOperator`,
which mirrors a dbt Cloud job run inside one Kubernetes pod — without dbt Cloud.

.. contents:: :local:


What the operator does
----------------------

In a single pod, in order:

1. **Clone** the dbt project from git (optional; the project may be baked into
   the image instead).  A git-repo cache on S3/GCS lets runs recover if git is
   temporarily unreachable.
2. **Install packages** — runs ``dbt deps`` (optional).
3. **Run your dbt commands** (``dbt build``, ``dbt test``, …) in the order
   given.  The first command to exit non-zero stops the run and fails the task.
4. **Upload** the ``target/`` directory to S3 or GCS on **both** success and
   failure, so ``run_results.json`` and the compiled SQL are always available
   for inspection.

The pod's exit code is the dbt exit code, so the Airflow task succeeds or fails
exactly as dbt did.  Because the operator subclasses
:class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`,
every KPO argument (``namespace``, ``container_resources``, ``deferrable``,
``secrets``, ``init_containers``, …) is accepted and passed straight through.


Installation
------------

.. code-block:: bash

    pip install apache-airflow-providers-dbt-core

For S3 artifact upload add the ``amazon`` extra; for GCS add ``google``:

.. code-block:: bash

    pip install "apache-airflow-providers-dbt-core[amazon]"
    pip install "apache-airflow-providers-dbt-core[google]"


Requirements
------------

.. list-table::
    :header-rows: 1
    :widths: 50 25 25

    * - PIP package
      - Used for
      - Requirement
    * - ``apache-airflow``
      - —
      - ``>=2.9.0``
    * - ``apache-airflow-providers-cncf-kubernetes``
      - running the pod
      - required
    * - ``apache-airflow-providers-amazon``
      - ``s3://`` artifact / git-cache upload
      - optional
    * - ``apache-airflow-providers-google``
      - ``gs://`` artifact / git-cache upload
      - optional


Quick start
-----------

Minimal example — dbt is already installed in the image:

.. code-block:: python

    from airflow.providers.dbt.core.operators.dbt import DbtKubernetesRunOperator

    run_dbt = DbtKubernetesRunOperator(
        task_id="run_dbt",
        image="my-registry/dbt-snowflake:1.8",
        namespace="airflow",
        steps=[
            "dbt build --select tag:hourly",
            "dbt test --select tag:hourly",
        ],
    )

One image for every adapter (recommended)
------------------------------------------

A single **generic** runner image — ``python`` + ``uv`` + ``git`` + ``aws``/``gsutil``
+ ``bash``, with **no dbt and no adapter baked in** — works for Snowflake,
BigQuery, Redshift, Databricks, Trino and every other adapter.  Each dbt
project carries its own ``uv.lock``, and the operator resolves dbt and the
adapter at run time:

.. code-block:: python

    from airflow.providers.dbt.core.operators.dbt import DbtKubernetesRunOperator

    run_dbt = DbtKubernetesRunOperator(
        task_id="run_dbt",
        # Generic image: python + uv + git + aws/gsutil. No dbt, no adapter.
        image="my-registry/dbt-runner-generic:latest",
        namespace="airflow",
        steps=[
            "dbt build --select tag:hourly",
            "dbt test --select tag:hourly",
        ],
        # Clone the project (which brings its own uv.lock).
        git_repo_url="github.com/acme/dbt-project.git",
        git_branch="main",
        git_conn_id="git_default",          # password field holds the token
        # uv run prepends itself to every step so dbt runs through the project venv.
        command_prefix="uv run",
        # Map connection fields to DBT_HOST / DBT_USER / DBT_PASSWORD / DBT_SCHEMA / DBT_PORT.
        warehouse_conn_id="snowflake_default",
        # Upload target/ to S3 on every exit (success and failure).
        artifact_dest="s3://my-dbt-artifacts/prod",
        artifact_conn_id="aws_default",
        deferrable=True,
    )

.. note::
    ``command_prefix="uv run"`` assumes the project's virtual environment already
    exists in the image (or was set up via an ``init_container``).  If you need
    to install dbt at run time, pass ``"uv sync && uv run dbt ..."`` as one of
    the ``steps`` or use an ``init_container`` to run the install before the main
    container starts.


Connections
-----------

git (``git_conn_id``)
    Any Airflow connection type.  Only the **password** field is used; it is
    injected as the HTTPS token for the clone and then scrubbed from the pod
    environment.

Warehouse (``warehouse_conn_id``)
    Any Airflow connection type.  The operator maps the standard fields to dbt
    ``env_var()`` environment variables:

    .. list-table::
        :header-rows: 1
        :widths: 30 70

        * - Connection field
          - Environment variable in pod
        * - ``host``
          - ``DBT_HOST``
        * - ``login``
          - ``DBT_USER``
        * - ``password``
          - ``DBT_PASSWORD``
        * - ``schema``
          - ``DBT_SCHEMA``
        * - ``port``
          - ``DBT_PORT``

    Reference these from your ``profiles.yml`` with ``env_var("DBT_HOST")``,
    ``env_var("DBT_USER")``, etc.

Artifact storage (``artifact_conn_id``)
    **S3** — an ``aws`` connection whose credentials have ``s3:PutObject`` on
    the target bucket.

    **GCS** — a ``google_cloud_default`` connection with a ``keyfile_dict``
    (JSON) in the **extra** field and the Storage Object Creator role on the
    bucket.


Artifact paths
--------------

``artifact_dest`` is a **base** prefix.  The operator appends a per-run
sub-path so that concurrent runs and retries never overwrite one another::

    <artifact_dest>/<dag_id>/<task-or-group-id>/<run_id>/attempt_<n>

For example, ``artifact_dest="s3://my-dbt-artifacts/prod"`` for a task
``run_dbt`` in DAG ``sales_hourly`` on its first attempt uploads to::

    s3://my-dbt-artifacts/prod/sales_hourly/run_dbt/<run_id>/attempt_1


Git repo caching
----------------

Set ``git_cache_dest`` to a base ``s3://`` or ``gs://`` prefix.  On a
successful clone the repo is compressed and uploaded so subsequent runs can
fall back to the cache if git is unreachable::

    git_cache_dest="s3://my-dbt-artifacts/git-cache",

The cache key is ``<git_cache_dest>/<dag_id>/<repo-slug>-<hash>/repo.tar.gz``,
unique per DAG + repo + branch, so multiple DAGs can share the same bucket
without colliding.


Advanced example: two-phase hourly pipeline
-------------------------------------------

A pre-processing phase for seeds and views, and a second phase that polls
for data readiness before running incremental models.  The AWS CLI is
side-loaded via an ``init_container`` so the runner image stays generic:

.. code-block:: python

    from datetime import timedelta

    from kubernetes.client import models as k8s
    from airflow import DAG
    from airflow.providers.dbt.core.operators.dbt import DbtKubernetesRunOperator
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.sensors.python import PythonSensor
    from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
    from airflow.sdk import TaskGroup

    DBT_IMAGE = "ghcr.io/astral-sh/uv:python3.12-bookworm"
    ARTIFACT_BASE = "s3://my-dbt-artifacts/prod"
    GIT_CACHE_BASE = "s3://my-dbt-artifacts/git-cache/prod"

    awscli_volume = k8s.V1Volume(name="aws-cli", empty_dir=k8s.V1EmptyDirVolumeSource())
    awscli_mount = k8s.V1VolumeMount(name="aws-cli", mount_path="/aws-cli")
    awscli_init = k8s.V1Container(
        name="install-awscli",
        image=DBT_IMAGE,
        command=["bash", "-c",
            "apt-get install -y -q --no-install-recommends curl unzip && "
            "ARCH=$(uname -m) && "
            'curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${ARCH}.zip"'
            ' -o /tmp/awscliv2.zip && '
            "unzip -q /tmp/awscliv2.zip -d /tmp/ && "
            "/tmp/aws/install --bin-dir /aws-cli/bin --install-dir /aws-cli/lib",
        ],
        volume_mounts=[awscli_mount],
    )

    k8s_kwargs = dict(
        image=DBT_IMAGE, namespace="airflow", kubernetes_conn_id="kubernetes_default",
        command_prefix="uv run",
        git_repo_url="github.com/acme/dbt-project.git", git_branch="main",
        git_conn_id="git_default", warehouse_conn_id="warehouse_default",
        artifact_dest=ARTIFACT_BASE, artifact_conn_id="aws_default",
        init_containers=[awscli_init], volumes=[awscli_volume], volume_mounts=[awscli_mount],
    )

    with DAG("sales_hourly", schedule="0 * * * *", catchup=False, max_active_runs=1) as dag:
        with TaskGroup("pre_processing") as pre_processing:
            DbtKubernetesRunOperator(
                task_id="seeds_and_views",
                steps=[
                    "dbt build --select 'config.materialized:view,path:seeds resource_type:seed'"
                    ' --vars \'{"data_interval_start": "{{ data_interval_start }}"}\' ',
                ],
                git_cache_dest=GIT_CACHE_BASE,
                **k8s_kwargs,
            )

        with TaskGroup("hourly_processing") as hourly_processing:
            wait = TimeDeltaSensor(task_id="wait_offset", delta=timedelta(minutes=10))
            sensor = PythonSensor(
                task_id="await_data",
                python_callable=lambda **kw: True,  # replace with real check
                poke_interval=180, timeout=5400,
            )
            run = DbtKubernetesRunOperator(
                task_id="incremental_models",
                steps=[
                    "dbt build --select tag:hourly"
                    ' --vars \'{"data_interval_start": "{{ data_interval_start }}"}\' ',
                ],
                **k8s_kwargs,
            )
            wait >> sensor >> run

        EmptyOperator(task_id="start") >> [pre_processing, hourly_processing]


Full documentation
------------------

`https://airflow.apache.org/docs/apache-airflow-providers-dbt-core/ <https://airflow.apache.org/docs/apache-airflow-providers-dbt-core/>`_
