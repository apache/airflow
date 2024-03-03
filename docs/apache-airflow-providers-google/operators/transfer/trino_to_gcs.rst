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


Trino to Google Cloud Storage Transfer Operator
===============================================

`Trino <https://trino.io/>`__ is an open source, fast, distributed SQL query engine for running interactive
analytic queries against data sources of all sizes ranging from gigabytes to petabytes. Trino allows
querying data where it lives, including Hive, Cassandra, relational databases or even proprietary data stores.
A single Trino query can combine data from multiple sources, allowing for analytics across your entire
organization.

`Google Cloud Storage <https://cloud.google.com/storage/>`__ allows world-wide storage and retrieval of
any amount of data at any time. You can use it to store backup and
`archive data <https://cloud.google.com/storage/archival>`__ as well
as a `data source for BigQuery <https://cloud.google.com/bigquery/external-data-cloud-storage>`__.


Data transfer
-------------

Transfer files between Trino and Google Storage is performed with the
:class:`~airflow.providers.google.cloud.transfers.trino_to_gcs.TrinoToGCSOperator` operator.

This operator has 3 required parameters:

* ``sql`` - The SQL to execute.
* ``bucket`` - The bucket to upload to.
* ``filename`` - The filename to use as the object name when uploading to Google Cloud Storage.
  A ``{}`` should be specified in the filename to allow the operator to inject file
  numbers in cases where the file is split due to size.

All parameters are described in the reference documentation - :class:`~airflow.providers.google.cloud.transfers.trino_to_gcs.TrinoToGCSOperator`.

An example operator call might look like this:

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_trino_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_trino_to_gcs_basic]
    :end-before: [END howto_operator_trino_to_gcs_basic]

Choice of data format
^^^^^^^^^^^^^^^^^^^^^

The operator supports two output formats:

* ``json`` - JSON Lines (default)
* ``csv``

You can specify these options by the ``export_format`` parameter.

If you want a CSV file to be created, your operator call might look like this:

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_trino_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_trino_to_gcs_csv]
    :end-before: [END howto_operator_trino_to_gcs_csv]

Generating BigQuery schema
^^^^^^^^^^^^^^^^^^^^^^^^^^

If you set ``schema_filename`` parameter, a ``.json`` file containing the BigQuery schema fields for the table
will be dumped from the database and upload to the bucket.

If you want to create a schema file, then an example operator call might look like this:

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_trino_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_trino_to_gcs_multiple_types]
    :end-before: [END howto_operator_trino_to_gcs_multiple_types]

For more information about the BigQuery schema, please look at
`Specifying schema <https://cloud.google.com/bigquery/docs/schemas>`__ in the Big Query documentation.

Division of the result into multiple files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This operator supports the ability to split large result into multiple files. The ``approx_max_file_size_bytes``
parameters allows developers to specify the file size of the splits. By default, the file has no more
than 1 900 000 000 bytes (1900 MB)

Check `Quotas & limits in Google Cloud Storage <https://cloud.google.com/storage/quotas>`__ to see the
maximum allowed file size for a single object.

If you want to create 10 MB files, your code might look like this:

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_trino_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_read_data_from_gcs_many_chunks]
    :end-before: [END howto_operator_read_data_from_gcs_many_chunks]

Querying data using the BigQuery
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The data available in Google Cloud Storage can be used by BigQuery. You can load data to BigQuery or
refer in queries directly to GCS data. For information about the loading data to the BigQuery, please look at
`Introduction to loading data from Cloud Storage <https://cloud.google.com/bigquery/docs/loading-data-cloud-storage>`__
in the BigQuery documentation. For information about the querying GCS data, please look at
`Querying Cloud Storage data <https://cloud.google.com/bigquery/docs/loading-data-cloud-storage>`__ in
the BigQuery documentation.

Airflow also has numerous operators that allow you to create the use of BigQuery.
For example, if you want to create an external table that allows you to create queries that
read data directly from GCS, then you can use :class:`~airflow.providers.google.cloud.operators.bigquery.BigQueryCreateExternalTableOperator`.
Using this operator looks like this:

.. exampleinclude:: /../../tests/system/providers/google/cloud/gcs/example_trino_to_gcs.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_external_table_multiple_types]
    :end-before: [END howto_operator_create_external_table_multiple_types]

For more information about the Airflow and BigQuery integration, please look at
the Python API Reference - :class:`~airflow.providers.google.cloud.operators.bigquery`.

Reference
^^^^^^^^^

For further information, look at:

* `Trino Documentation <https://trinodb.io//docs/current/>`__

* `Google Cloud Storage Documentation <https://cloud.google.com/storage/docs/>`__
