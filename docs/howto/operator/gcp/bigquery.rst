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

Google Cloud BigQuery Operators
===============================

`BigQuery <https://cloud.google.com/bigquery/>`__ is Google's fully managed, petabyte
scale, low cost analytics data warehouse. It is a serverless Software as a Service
(SaaS) that doesn't need a database administrator. It allows users to focus on
analyzing data to find meaningful insights using familiar SQL.

Airflow provides operators to manage datasets and tables, run queries and validate
data.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: _partials/prerequisite_tasks.rst

.. _howto/operator:BigQueryCreateEmptyDatasetOperator:
.. _howto/operator:BigQueryGetDatasetOperator:
.. _howto/operator:BigQueryGetDatasetTablesOperator:
.. _howto/operator:BigQueryPatchDatasetOperator:
.. _howto/operator:BigQueryUpdateDatasetOperator:
.. _howto/operator:BigQueryDeleteDatasetOperator:

Manage datasets
^^^^^^^^^^^^^^^

Create dataset
""""""""""""""

To create an empty dataset in a BigQuery database you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyDatasetOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_create_dataset]
    :end-before: [END howto_operator_bigquery_create_dataset]

Get dataset details
"""""""""""""""""""

To get the details of an existing dataset you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryGetDatasetOperator`.

This operator returns a `Dataset Resource <https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#resource>`__.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_get_dataset]
    :end-before: [END howto_operator_bigquery_get_dataset]

List tables in dataset
""""""""""""""""""""""

To retrieve the list of tables in a given dataset use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryGetDatasetTablesOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_get_dataset_tables]
    :end-before: [END howto_operator_bigquery_get_dataset_tables]

Patch dataset
"""""""""""""

To patch a dataset in BigQuery you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryPatchDatasetOperator`.

Note, this operator only replaces fields that are provided in the submitted dataset
resource.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_patch_dataset]
    :end-before: [END howto_operator_bigquery_patch_dataset]

Update dataset
""""""""""""""

To update a dataset in BigQuery you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryUpdateDatasetOperator`.

The update method replaces the entire dataset resource, whereas the patch
method only replaces fields that are provided in the submitted dataset resource.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_update_dataset]
    :end-before: [END howto_operator_bigquery_update_dataset]

Delete dataset
""""""""""""""

To delete an existing dataset from a BigQuery database you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteDatasetOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_delete_dataset]
    :end-before: [END howto_operator_bigquery_delete_dataset]

.. _howto/operator:BigQueryCreateEmptyTableOperator:
.. _howto/operator:BigQueryCreateExternalTableOperator:
.. _howto/operator:BigQueryGetDataOperator:
.. _howto/operator:BigQueryUpsertTableOperator:
.. _howto/operator:BigQueryDeleteTableOperator:

Manage tables
^^^^^^^^^^^^^

Create native table
"""""""""""""""""""

To create a new, empty table in the given BigQuery dataset, optionally with
schema you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyTableOperator`.

The schema to be used for the BigQuery table may be specified in one of two
ways. You may either directly pass the schema fields in, or you may point the
operator to a Google Cloud Storage object name. The object in Google Cloud
Storage must be a JSON file with the schema fields in it.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_create_table]
    :end-before: [END howto_operator_bigquery_create_table]

You can use this operator to create a view on top of an existing table.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_create_view]
    :end-before: [END howto_operator_bigquery_create_view]

Create external table
"""""""""""""""""""""

To create a new external table in a dataset with the data in Google Cloud Storage
you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryCreateExternalTableOperator`.

Similarly to
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryCreateEmptyTableOperator`
you may either directly pass the schema fields in, or you may point the operator
to a Google Cloud Storage object name.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_create_external_table]
    :end-before: [END howto_operator_bigquery_create_external_table]

Fetch data from table
"""""""""""""""""""""

To fetch data from a BigQuery table you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator`.
Alternatively you can fetch data for selected columns if you pass fields to
``selected_fields``.

This operator returns data in a Python list where the number of elements in the
returned list will be equal to the number of rows fetched. Each element in the
list will again be a list where elements would represent the column values for
that row.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_get_data]
    :end-before: [END howto_operator_bigquery_get_data]

Upsert table
""""""""""""

To upsert a table you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryUpsertTableOperator`.

This operator either updates the existing table or creates a new, empty table
in the given dataset.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_upsert_table]
    :end-before: [END howto_operator_bigquery_upsert_table]

Delete table
""""""""""""

To delete an existing table you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteTableOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_delete_table]
    :end-before: [END howto_operator_bigquery_delete_table]

You can also use this operator to delete a view.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_delete_view]
    :end-before: [END howto_operator_bigquery_delete_view]

.. _howto/operator:BigQueryExecuteQueryOperator:

Execute queries
^^^^^^^^^^^^^^^

Let's say you would like to execute the following query.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_bigquery_query]
    :end-before: [END howto_operator_bigquery_query]

To execute the SQL query in a specific BigQuery database you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_execute_query]
    :end-before: [END howto_operator_bigquery_execute_query]

``sql`` argument can receive a str representing a sql statement, a list of str
(sql statements), or reference to a template file. Template reference are recognized
by str ending in '.sql'.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_execute_query_list]
    :end-before: [END howto_operator_bigquery_execute_query_list]

You can store the results of the query in a table by specifying
``destination_dataset_table``.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_execute_query_save]
    :end-before: [END howto_operator_bigquery_execute_query_save]

.. _howto/operator:BigQueryCheckOperator:
.. _howto/operator:BigQueryValueCheckOperator:
.. _howto/operator:BigQueryIntervalCheckOperator:

Validate data
^^^^^^^^^^^^^

Check if query result has data
""""""""""""""""""""""""""""""

To perform checks against BigQuery you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator`.

This operator expects a sql query that will return a single row. Each value on
that first row is evaluated using python ``bool`` casting. If any of the values
return ``False`` the check is failed and errors out.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_check]
    :end-before: [END howto_operator_bigquery_check]

Compare query result to pass value
""""""""""""""""""""""""""""""""""

To perform a simple value check using sql code you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator`.

This operator expects a sql query that will return a single row. Each value on
that first row is evaluated against ``pass_value`` which can be either a string
or numeric value. If numeric, you can also specify ``tolerance``.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_value_check]
    :end-before: [END howto_operator_bigquery_value_check]

Compare metrics over time
"""""""""""""""""""""""""

To check that the values of metrics given as SQL expressions are within a certain
tolerance of the ones from ``days_back`` before you can use
:class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator`.

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigquery.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_bigquery_interval_check]
    :end-before: [END howto_operator_bigquery_interval_check]

Reference
^^^^^^^^^

For further information, look at:

* `Client Library Documentation <https://googleapis.dev/python/bigquery/latest/index.html>`__
* `Product Documentation <https://cloud.google.com/bigquery/docs/>`__
