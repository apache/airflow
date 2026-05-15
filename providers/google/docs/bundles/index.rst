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

A Dag bundle is a way to load Dags into Airflow from an external source. For a general overview of
Dag bundles, see :doc:`apache-airflow:administration-and-deployment/dag-bundles`.

GCSDagBundle
============

Use the :class:`~airflow.providers.google.cloud.bundles.gcs.GCSDagBundle` to load Dags directly from
a Google Cloud Storage bucket. Airflow will periodically sync Dag files from the specified GCS bucket
and prefix to a local directory and load them from there. This bundle does not support versioning.

Prerequisites
-------------

- A Google Cloud connection configured in Airflow (see :doc:`/connections/gcp`).
- A GCS bucket containing your Dag Python files.

Configuration
-------------

Add the bundle to your ``[dag_processor] dag_bundle_config_list`` configuration:

.. code-block:: json

    [
      {
        "name": "my-gcs-dags",
        "classpath": "airflow.providers.google.cloud.bundles.gcs.GCSDagBundle",
        "kwargs": {
          "gcp_conn_id": "google_cloud_default",
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
        "name": "my-gcs-dags",
        "classpath": "airflow.providers.google.cloud.bundles.gcs.GCSDagBundle",
        "kwargs": {
          "gcp_conn_id": "google_cloud_default",
          "bucket_name": "my-airflow-bucket",
          "prefix": "dags/",
          "refresh_interval": 60
        }
      }
    ]'

Parameters
----------

- ``gcp_conn_id`` – Airflow connection ID for Google Cloud. Defaults to ``google_cloud_default``.
- ``bucket_name`` – Name of the GCS bucket containing the Dag files.
- ``prefix`` – Optional subdirectory prefix within the bucket. If omitted, Dags are loaded from the root of the bucket.
- ``refresh_interval`` – How often (in seconds) to sync Dags from GCS.
