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

MongoDB To Google Cloud Storage Operator
========================================
The `Google Cloud Storage <https://cloud.google.com/storage/>`__ (GCS) service is
used to store large data from various applications. This page shows how to copy
data from MongoDB to GCS.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:MongoToGCSOperator:

MongoToGCSOperator
~~~~~~~~~~~~~~~~~~

:class:`~airflow.providers.google.cloud.transfers.mongo_to_gcs.MongoToGCSOperator` allows you to upload
data from a MongoDB collection to GCS in JSON, CSV, or Parquet format.

The operator accepts either a ``find()`` filter (a ``dict``) or an aggregation pipeline (a ``list``)
through the ``mongo_query`` parameter. When ``mongo_query`` is a dict, ``mongo_projection`` may be
used to limit the fields returned. When ``mongo_query`` is a list, the value is passed as an
aggregation pipeline and ``mongo_projection`` is ignored.

The schema is derived from the first document in the result set, so all documents are expected to
share a consistent shape; missing fields are exported as ``null``.

Below is an example of using this operator to export the result of a ``find()`` query to GCS.

.. exampleinclude:: /../../google/tests/system/google/cloud/transfers/example_mongo_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_mongo_to_gcs]
    :end-before: [END howto_operator_mongo_to_gcs]

The operator also supports running an aggregation pipeline.

.. exampleinclude:: /../../google/tests/system/google/cloud/transfers/example_mongo_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_mongo_to_gcs_aggregation]
    :end-before: [END howto_operator_mongo_to_gcs_aggregation]


Reference
---------

For further information, look at:

* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
* `MongoDB Documentation <https://www.mongodb.com/docs/>`__
