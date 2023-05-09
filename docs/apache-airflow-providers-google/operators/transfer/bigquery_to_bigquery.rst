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


Google Cloud BigQuery Transfer Operator to BigQuery
===================================================

`Google Cloud BigQuery <https://cloud.google.com/bigquery>`__ is Google Cloud's serverless
data warehouse offering.
This operator can be used to copy data from one BigQuery table to another.


Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst

.. _howto/operator:BigQueryToBigQueryOperator:

Operator
^^^^^^^^

Copying data from one BigQuery table to another is performed with the
:class:`~airflow.providers.google.cloud.transfers.bigquery_to_bigquery.BigQueryToBigQueryOperator` operator.

Use :ref:`Jinja templating <concepts:jinja-templating>` with
:template-fields:`airflow.providers.google.cloud.transfers.bigquery_to_bigquery.BigQueryToBigQueryOperator`
to define values dynamically.

You may include multiple source tables, as well as define a ``write_disposition`` and a ``create_disposition``.
For more information, please refer to the links above.


Copying BigQuery tables
-----------------------

The following Operator copies data from one or more BigQuery tables to another.

.. exampleinclude:: /../../tests/system/providers/google/cloud/bigquery/example_bigquery_to_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_to_bigquery]
    :end-before: [END howto_operator_bigquery_to_bigquery]


Reference
^^^^^^^^^

For further information, look at:

* `Google Cloud Storage Documentation <https://cloud.google.com/storage/>`__
* `Google Cloud BigQuery Documentation <https://cloud.google.com/bigquery/docs/managing-tables#copy-table>`__
