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

A DAG bundle is a way to load DAGs into Airflow from an external source. For a general overview of
DAG bundles, see :doc:`apache-airflow:administration-and-deployment/dag-bundles`.

S3DagBundle
===========

Use the :class:`~airflow.providers.amazon.aws.bundles.s3.S3DagBundle` to load DAGs directly from
an Amazon S3 bucket. Airflow will periodically sync DAG files from the specified S3 bucket and prefix
to a local directory and load them from there.

Prerequisites
-------------

- An AWS connection configured in Airflow (see :doc:`/connections/aws`).
- An S3 bucket containing your DAG Python files.

Configuration
-------------

Add the bundle to your ``[dag_processor] dag_bundle_config_list`` configuration:

.. code-block:: json

    [
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
    ]

Or using an environment variable:

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

Parameters
----------

- ``aws_conn_id`` – Airflow connection ID for AWS. Defaults to ``aws_default``.
- ``bucket_name`` – Name of the S3 bucket containing the DAG files.
- ``prefix`` – Optional subdirectory prefix within the bucket. If omitted, DAGs are loaded from the root of the bucket.
- ``refresh_interval`` – How often (in seconds) to sync DAGs from S3.
