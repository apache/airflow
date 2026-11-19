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

Bundles
#######

Dag bundles allow Airflow to load Dags from external sources. For a general overview see
:doc:`apache-airflow:administration-and-deployment/dag-bundles`.

S3DagBundle
===========

Use the :class:`~airflow.providers.amazon.aws.bundles.s3.S3DagBundle` to configure an S3 bundle in your Airflow's
``[dag_processor] dag_bundle_config_list``.

The ``prefix`` selects a subdirectory, with or without a trailing slash. For example,
``dags`` and ``dags/`` both download objects under ``dags/``. An empty prefix downloads
the whole bucket. IAM policies that restrict listing with the ``s3:prefix`` condition
must allow the directory prefix including its trailing slash.

Example of using the S3DagBundle:

**JSON format example**:

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
      {
        "name": "my-s3-dags",
        "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
        "kwargs": {
          "aws_conn_id": "aws_default",
          "bucket_name": "my-airflow-bucket",
          "prefix": "dags/",
          "refresh_interval": 60
        }
      }
    ]'

S3ArchiveDagBundle
==================

Use :class:`~airflow.providers.amazon.aws.bundles.s3.S3ArchiveDagBundle` when the Dags are published as one
``.tar.gz`` object. :class:`~airflow.providers.amazon.aws.bundles.s3.S3DagBundle` downloads every object under
``prefix`` one at a time. Each object costs a full request round-trip, so bundles made of many small files
can take a long time to stage — a cost paid by every component that stages the bundle, which with ephemeral
workers (e.g. KubernetesExecutor task pods) means on every task start.

``S3ArchiveDagBundle`` stages the bundle from the ``archive_key`` object: one ``HEAD`` request to detect
changes (unchanged archives are not re-downloaded), one ``GET`` to fetch it, then a local unpack and a swap
into place. The archive is the only source of the Dags. Configure this class instead of ``S3DagBundle``;
there is no ``prefix`` argument and no second copy to keep in sync.

Archive mode requires Python 3.11.4 or later, because unpacking uses ``tarfile`` ``filter="data"``
(added in that release). Constructing ``S3ArchiveDagBundle`` on an older interpreter raises
``RuntimeError``.

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
      {
        "name": "my-s3-dags",
        "classpath": "airflow.providers.amazon.aws.bundles.s3.S3ArchiveDagBundle",
        "kwargs": {
          "aws_conn_id": "aws_default",
          "bucket_name": "my-airflow-bucket",
          "archive_key": "bundle-archives/dags.tar.gz",
          "refresh_interval": 60
        }
      }
    ]'

Publishing the archive is the responsibility of your deployment process. Members are extracted relative to
the bundle root, so pack the contents of the Dags directory rather than the directory itself — for example:

.. code-block:: bash

    tar -C ./dags -czf dags.tar.gz .
    aws s3 cp dags.tar.gz s3://my-airflow-bucket/bundle-archives/dags.tar.gz
