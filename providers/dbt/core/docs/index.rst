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
which runs a dbt "job" — clone, ``dbt deps``, your dbt commands, and an
artifact upload — inside one Kubernetes pod, mirroring the shape of a dbt Cloud
job run without requiring dbt Cloud.

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Basics

    Home <self>
    Changelog <changelog>
    Security <security>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Guides

    Connections <connections>
    Operators <operators/dbt>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: References

    Python API <_api/airflow/providers/dbt/core/index>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: System tests

    System Tests <_api/tests/system/dbt/core/index>

``apache-airflow-providers-dbt-core`` package
---------------------------------------------

Release: 1.0.0

Provider package
----------------

This package is for the ``dbt.core`` provider. All classes for this package are
included in the ``airflow.providers.dbt.core`` python package.

What the operator does
----------------------

:class:`~airflow.providers.dbt.core.operators.dbt.DbtKubernetesRunOperator`
subclasses
:class:`~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`
and runs the following lifecycle in a single pod, in order:

#. **Clone** the dbt project from git (optional — the project may instead be
   baked into the image).
#. **Install packages** with ``dbt deps`` (optional).
#. **Run your dbt commands** (``dbt build``, ``dbt test``, ...) in the order
   given, stopping at the first failure.
#. **Upload** the ``target/`` directory to S3 or GCS. The upload runs on
   **both** success and failure, so ``run_results.json``, ``manifest.json`` and
   the compiled SQL are always available for inspection.

The pod's exit code is the dbt exit code, so the Airflow task succeeds or fails
exactly as dbt did. Because the operator is a thin subclass, every
``KubernetesPodOperator`` argument (``namespace``, ``container_resources``,
``deferrable``, ``secrets``, ``env_from``, ...) is accepted and passed straight
through.

One image for every adapter
---------------------------

You do not need a separate runner image per warehouse. The recommended setup is
a single **generic** image — ``python`` + ``uv`` + ``git`` + ``aws``/``gsutil``
+ ``bash`` + ``curl`` + ``tar``, with **no dbt and no adapter baked in** — that
serves Snowflake, BigQuery, Redshift, Databricks, Postgres, Trino and the rest.
Each dbt project brings its own dbt version, adapter and packages, and the
operator resolves them at run time. There are two dependency models:

.. list-table::
    :header-rows: 1
    :widths: 25 75

    * - Model
      - How dependencies get into the pod
    * - **Runtime ``uv sync`` (recommended)**
      - The generic image carries only ``uv`` and the system CLIs. Set
        ``install_command="uv sync --frozen"`` to install dbt + the adapter +
        packages from the project's own ``uv.lock`` at run time, and
        ``command_prefix="uv run"`` so dbt runs through that environment. One
        image works for every adapter; a project upgrades dbt simply by editing
        its lockfile.
    * - **Baked adapter**
      - dbt Core and one warehouse adapter are installed into the image and are
        on ``PATH``. Omit ``install_command`` and ``command_prefix``. This
        needs a per-adapter image but does not resolve dependencies on each run.

See :doc:`operators/dbt` for a full how-to, and :doc:`connections` for how the
git, warehouse and artifact connections are used.

Installation
------------

You can install this provider on top of an existing Airflow installation via
``pip install apache-airflow-providers-dbt-core``. For the Airflow versions
supported, see ``Requirements`` below.

Requirements
------------

This provider supports both **Apache Airflow 2 and Airflow 3**.

Executing the dbt job requires the Kubernetes provider, which is a hard
dependency. Artifact upload to object storage is optional and pulls in the
relevant cloud provider only when it is actually used:

.. list-table::
    :header-rows: 1
    :widths: 45 30 25

    * - PIP package
      - Used for
      - Requirement
    * - ``apache-airflow-providers-cncf-kubernetes``
      - running the pod
      - required
    * - ``apache-airflow-providers-amazon``
      - ``s3://`` artifact upload
      - optional
    * - ``apache-airflow-providers-google``
      - ``gs://`` artifact upload
      - optional

The optional cloud providers are imported lazily. If you request an ``s3://``
or ``gs://`` ``artifact_dest`` without the matching provider installed, the
task raises ``AirflowOptionalProviderFeatureException``.
