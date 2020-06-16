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


Downloads data from Google Cloud Storage to Local Filesystem
============================================================
The `Google Cloud Storage <https://cloud.google.com/storage/>`__  (GCS) is used to store large data from various applications.
This page shows how to download data from GCS to local filesystem.

.. contents::
  :depth: 1
  :local:


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst

.. _howto/operator:GCSToLocalFilesystemOperator:

GCSToLocalFilesystemOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`~airflow.providers.google.cloud.transfers.gcs_to_local.GCSToLocalFilesystemOperator` allows you to download
data from GCS to local filesystem.


Below is an example of using this operator to upload a file to GCS.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_gcs.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_gcs_download_file_task]
    :end-before: [END howto_operator_gcs_download_file_task]


Reference
---------

For further information, look at:

* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
