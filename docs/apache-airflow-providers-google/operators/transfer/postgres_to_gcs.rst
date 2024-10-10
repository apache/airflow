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

Postgres To Google Cloud Storage Operator
=========================================
The `Google Cloud Storage <https://cloud.google.com/storage/>`__ (GCS) service is
used to store large data from various applications. This page shows how to copy
data from Postgres to GCS.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:PostgresToGCSOperator:

PostgresToGCSOperator
~~~~~~~~~~~~~~~~~~~~~

:class:`~airflow.providers.google.cloud.transfers.postgres_to_gcs.PostgresToGCSOperator` allows you to upload
data from Postgres database to GCS.

When you use this operator, you can optionally compress the data being uploaded to gzip format.

Below is an example of using this operator to upload data to GCS.

.. exampleinclude:: /../../providers/tests/system/google/cloud/transfers/example_postgres_to_gcs.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_postgres_to_gcs]
    :end-before: [END howto_operator_postgres_to_gcs]


Reference
---------

For further information, look at:
* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
