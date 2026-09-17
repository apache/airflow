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

.. _howto/operator:DbtKubernetesRunOperator:

DbtKubernetesRunOperator
========================

Use the
:class:`~airflow.providers.dbt.core.operators.dbt.DbtKubernetesRunOperator` to
run a dbt Core job inside a single Kubernetes pod. One pod clones the project
(optional), installs the project's dependencies (optional), runs ``dbt deps``
(optional), executes your dbt commands in order, and uploads the ``target/``
directory to object storage on **both** success and failure. The pod's exit
code is the dbt exit code, so the Airflow task reflects the true outcome of the
run.

The operator subclasses
:class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`,
so any of its arguments (for example ``namespace``, ``container_resources``,
``secrets``, ``env_from`` or ``deferrable``) can be supplied and are passed
straight through.

.. _howto/operator:DbtKubernetesRunOperator:generic-image:

One image for every adapter (recommended)
-----------------------------------------

The recommended way to run this operator is with a single **generic** runner
image and to install dbt at run time from the dbt project's own lockfile. The
image bakes in only the tooling that the lifecycle script needs — ``python`` +
``uv`` + ``git`` + ``aws``/``gsutil`` + ``bash`` + ``curl`` + ``tar`` — and
**no dbt and no adapter**. Two parameters wire it up:

* ``install_command="uv sync --frozen"`` — run once in the project directory,
  after the clone and before dbt, to install dbt Core, the warehouse adapter
  and any Python packages exactly as pinned in the project's ``uv.lock``.
* ``command_prefix="uv run"`` — prepended to ``dbt deps`` and to every dbt
  command so they execute inside the virtual environment that ``uv sync``
  created.

Because dbt and the adapter come from each project's lockfile at run time, the
**same image serves every warehouse** (Snowflake, BigQuery, Redshift,
Databricks, Postgres, Trino, ...). There is no per-adapter image to build,
publish, or keep patched, and a project upgrades dbt or its adapter simply by
updating its own ``uv.lock``.

.. code-block:: python

    from airflow.providers.dbt.core.operators.dbt import DbtKubernetesRunOperator

    run_dbt = DbtKubernetesRunOperator(
        task_id="run_dbt",
        # Generic base image: python + uv + git + aws/gsutil. No dbt, no adapter.
        image="example.com/dbt-runner:latest",
        dbt_commands=[
            "dbt build --select tag:hourly",
            "dbt test --select tag:hourly",
        ],
        # Clone the dbt project (which carries its own uv.lock).
        git_repo_url="github.com/acme/dbt-project.git",
        git_branch="main",
        git_conn_id="acme_git",
        # Install dbt + adapter + packages from the project's uv.lock at run time,
        # then invoke dbt through that environment.
        install_command="uv sync --frozen",
        command_prefix="uv run",
        # profiles.yml env_var() lookups (DBT_HOST/DBT_USER/... ).
        warehouse_conn_id="snowflake_default",
        # Base prefix; the operator appends a per-run path (see artifact_dest).
        artifact_dest="s3://my-dbt-artifacts",
        artifact_conn_id="aws_default",
        namespace="airflow",
    )

.. note::

    The other supported model is a **baked-adapter** image: dbt Core and one
    warehouse adapter are installed into the image and are on ``PATH``. In that
    case omit both ``install_command`` and ``command_prefix`` — dbt runs
    directly. This trades the "one image for every adapter" benefit for not
    resolving dependencies on each run. The two models are compared in
    :doc:`the package overview </index>`.

Example
-------

The following end-to-end example is also the provider's system test. It clones
a project from git, resolves warehouse credentials from an Airflow connection,
runs the dbt commands, and uploads ``target/`` to S3 on success and failure:

.. exampleinclude:: /../tests/system/dbt/core/example_dbt_kubernetes.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dbt_kubernetes]
    :end-before: [END howto_operator_dbt_kubernetes]

Parameters
----------

``dbt_commands`` (required)
    The dbt CLI commands to run, in order, for example
    ``["dbt build --select tag:hourly", "dbt test"]``. Each command runs inside
    the pod; the first command to exit non-zero stops the run and fails the
    task. When ``command_prefix`` is set it is prepended to each command. Must
    be a non-empty list.

``image`` (required)
    The runner image. With the recommended **generic** model the image needs
    only ``python`` + ``uv`` + ``git`` + ``bash`` + the object-storage CLI used
    for upload (``aws`` for S3 or ``gsutil`` for GCS); dbt and the adapter are
    installed at run time via ``install_command``. With the **baked-adapter**
    model the image must additionally contain dbt and the relevant adapter on
    ``PATH``.

``project_dir``
    Path to the dbt project inside the pod (default ``/dbt``). When a repo is
    cloned it is cloned to this path; otherwise the project must already exist
    here in the image. ``install_command`` runs in this directory.

``install_command``
    Shell command run once in ``project_dir`` **after** the clone and
    **before** ``dbt deps`` and the dbt commands, for example
    ``"uv sync --frozen"``. Use it to install the project's own dependencies
    (dbt Core, the warehouse adapter and any Python packages) at run time so a
    single generic image can serve every adapter. If omitted, dbt and the
    adapter must already be present in the image.

``command_prefix``
    A prefix prepended to ``dbt deps`` and to every command in ``dbt_commands``,
    for example ``"uv run"`` when ``install_command`` installed dbt into a
    project virtual environment. Default is empty, which assumes ``dbt`` is on
    ``PATH`` (the baked-adapter model).

``install_deps``
    Run ``dbt deps`` before the commands (default ``True``). It is invoked as
    ``<command_prefix> dbt deps``.

Cloning the project — ``git_repo_url`` / ``git_branch`` / ``git_conn_id``
    Set ``git_repo_url`` to the host/path of the repo to clone, e.g.
    ``github.com/acme/dbt-project.git``, and optionally ``git_branch``
    (default ``main``). If ``git_repo_url`` is omitted, the project is assumed
    to be baked into the image at ``project_dir``. ``git_conn_id`` names an
    Airflow connection whose **password** holds the git access token; it is
    injected only for the clone. See :doc:`/connections`.

``warehouse_conn_id``
    An Airflow connection whose fields are mapped to the ``DBT_HOST``,
    ``DBT_USER``, ``DBT_PASSWORD``, ``DBT_SCHEMA`` and ``DBT_PORT`` environment
    variables in the pod. Reference them from your project's ``profiles.yml``
    with dbt's ``env_var()`` so that credentials never live in the image. See
    :doc:`/connections`.

Uploading artifacts — ``artifact_dest`` / ``artifact_conn_id``
    Set ``artifact_dest`` to a **base** ``s3://bucket/prefix`` or
    ``gs://bucket/prefix`` location to receive the ``target/`` directory. The
    operator does not upload to that prefix verbatim; it appends a per-run
    path so that concurrent runs and retries never overwrite one another::

        <artifact_dest>/<dag_id>/<task-or-group-id>/<run_id>/attempt_<n>

    The ``<task-or-group-id>`` segment is the enclosing task group's id when the
    task is inside one, otherwise the task id, and ``<n>`` is the attempt
    (try) number. For example, a base of ``s3://my-dbt-artifacts`` for a task
    ``run_dbt`` in DAG ``sales_hourly`` on its first attempt uploads to
    ``s3://my-dbt-artifacts/sales_hourly/run_dbt/<run_id>/attempt_1``.

    The upload runs on both success and failure. ``artifact_conn_id`` names the
    AWS or Google connection used to resolve upload credentials; the scheme of
    ``artifact_dest`` selects the hook (``s3://`` uses ``S3Hook``, ``gs://``
    uses ``GCSHook``). If ``artifact_dest`` is omitted, nothing is uploaded.

    .. note::

        Upload credentials are resolved on the worker and injected into the pod
        as environment variables. Read :doc:`/security` before using this in
        production.

``deferrable``
    ``DbtKubernetesRunOperator`` adds no trigger of its own; ``deferrable`` is
    the standard ``KubernetesPodOperator`` passthrough. When ``True``, the
    worker slot is released while the pod runs and pod completion is awaited on
    the triggerer.
