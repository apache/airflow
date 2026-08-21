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

WasbDagBundle
=============

Use the :class:`~airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle` to configure an Azure Blob
Storage bundle in your Airflow's ``[dag_processor] dag_bundle_config_list``. The bundle does not support
versioning; tasks always use the latest blobs synced to the local bundle directory.

Example of using the WasbDagBundle:

**JSON format example**:

.. code-block:: bash

    export AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
     {
         "name": "my-wasb-dags",
         "classpath": "airflow.providers.microsoft.azure.bundles.wasb.WasbDagBundle",
         "kwargs": {
             "wasb_conn_id": "wasb_default",
             "container_name": "airflow-dags",
             "prefix": "dags/",
             "refresh_interval": 60
         }
     }
    ]'

Authentication
--------------

The bundle uses a ``wasb`` Connection (``wasb_conn_id``). Authentication is the same as for
:class:`~airflow.providers.microsoft.azure.hooks.wasb.WasbHook` — see :doc:`../connections/wasb`. On Azure-hosted
Airflow, managed identity via ``DefaultAzureCredential`` is typical.

Permissions
-----------

The identity needs read access to list and download blobs in the target container. Assign
`Storage Blob Data Reader <https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/storage#storage-blob-data-reader>`_
at the storage account or container scope.

Container and prefix
--------------------

Set ``container_name`` to the blob container that holds your Dag files. Use ``prefix`` for an optional
virtual folder inside the container.

Networking
----------

The Dag processor needs outbound HTTPS to the blob endpoint. Storage firewalls and private endpoints
must allow access from Airflow, as for any WASB client.

Reusing the Connection in Dags
------------------------------

You can use the same ``wasb`` Connection ID in ``wasb_conn_id`` for the bundle and for operators or sensors
that use ``WasbHook``.
