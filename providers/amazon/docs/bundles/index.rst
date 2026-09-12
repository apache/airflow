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

Staging from a single archive object
------------------------------------

By default the bundle is staged by downloading every object under ``prefix`` one at a time. Each object
costs a full request round-trip, so bundles made of many small files can take a long time to stage — a cost
paid by every component that stages the bundle, which with ephemeral workers (e.g. KubernetesExecutor task
pods) means on every task start.

Setting the optional ``archive_key`` stages the bundle from a single ``.tar.gz`` object instead: one
``HEAD`` request to detect changes (unchanged archives are not re-downloaded), one ``GET`` to fetch it, then
a local unpack and an atomic swap into place. If the archive cannot be fetched or unpacked, staging
automatically falls back to the per-object sync of ``prefix``.

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
      {
        "name": "my-s3-dags",
        "classpath": "airflow.providers.amazon.aws.bundles.s3.S3DagBundle",
        "kwargs": {
          "aws_conn_id": "aws_default",
          "bucket_name": "my-airflow-bucket",
          "prefix": "dags/",
          "archive_key": "bundle-archives/dags.tar.gz",
          "refresh_interval": 60
        }
      }
    ]'

Publishing the archive is the responsibility of your deployment process. The archive members must be laid out exactly as
the objects under ``prefix``, so both staging strategies produce the same local tree — for example, in the
same CI job that syncs the Dags:

.. code-block:: bash

    tar -C ./dags -czf dags.tar.gz .
    aws s3 cp dags.tar.gz s3://my-airflow-bucket/bundle-archives/dags.tar.gz

.. note::
    Keep the archive outside the ``prefix`` location, otherwise a ``aws s3 sync --delete`` of the Dag
    files may remove it.
