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


Google Cloud BigQuery Transfer Operator to Google Cloud Storage
===============================================================

`Google Cloud BigQuery <https://cloud.google.com/bigquery>`__ is Google Cloud's serverless
data warehouse offering.
`Google Cloud Storage (GCS) <https://cloud.google.com/storage/>`__ is a managed service for
storing unstructured data.
This operator can be used to export data from BigQuery tables into files in a
Cloud Storage bucket.


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:BigQueryToGCSOperator:

Operator
^^^^^^^^

File transfer from GCS to BigQuery is performed with the
:class:`~airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryToGCSOperator` operator.

Use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryToGCSOperator`
to define values dynamically.

You may define multiple destination URIs, as well as other settings such as ``compression`` and
``export_format``. For more information, please refer to the links above.


Importing files
---------------

The following Operator imports one or more files from GCS into a BigQuery table.

.. exampleinclude:: /../../tests/system/providers/google/cloud/bigquery/example_bigquery_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_to_gcs]
    :end-before: [END howto_operator_bigquery_to_gcs]


Reference
^^^^^^^^^

For further information, look at:

* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
* `Google Cloud BigQuery Documentation <https://cloud.google.com/bigquery/docs/exporting-data>`__
