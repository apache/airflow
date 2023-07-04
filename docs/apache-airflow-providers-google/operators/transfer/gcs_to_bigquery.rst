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


Google Cloud Storage Transfer Operator to BigQuery
==================================================

`Google Cloud Storage (GCS) <https://cloud.google.com/storage/>`__ is a managed service for
storing unstructured data.
`Google Cloud BigQuery <https://cloud.google.com/bigquery>`__ is Google Cloud's serverless
data warehouse offering.
This operator can be used to populate BigQuery tables with data from files stored in a
Cloud Storage bucket.


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:GCSToBigQueryOperator:

Operator
^^^^^^^^

File transfer from GCS to BigQuery is performed with the
:class:`~airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator` operator.

Use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator`
to define values dynamically.

You may load multiple objects from a single bucket using the ``source_objects`` parameter.
You may also define a schema, as well as additional settings such as the compression format.
For more information, please refer to the links above.

Transferring files
------------------

The following Operator transfers one or more files from GCS into a BigQuery table.

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_gcs_to_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_bigquery]
    :end-before: [END howto_operator_gcs_to_bigquery]

Also you can use GCSToBigQueryOperator in the deferrable mode:

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_gcs_to_bigquery_async.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcs_to_bigquery_async]
    :end-before: [END howto_operator_gcs_to_bigquery_async]


Reference
^^^^^^^^^

For further information, look at:

* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
* `Google Cloud BigQuery Documentation <https://cloud.google.com/bigquery/docs/batch-loading-data>`__
