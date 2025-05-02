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

Google Cloud Storage to Presto Transfer Operator
================================================

Google has a service `Google Cloud Storage <https://cloud.google.com/storage/>`__. This service is
used to store large data from various applications.

`Presto <https://prestodb.io/>`__ is an open source distributed SQL query engine for running interactive
analytic queries against data sources of all sizes ranging from gigabytes to petabytes. Presto allows
querying data where it lives, including Hive, Cassandra, relational databases or even proprietary data stores.
A single Presto query can combine data from multiple sources, allowing for analytics across your entire
organization.

.. _howto/operator:GCSToPresto:

Load CSV from GCS to Presto Table
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To load a CSV file from Google Cloud Storage to a Presto table you can use the
:class:`~airflow.providers.presto.transfers.gcs_to_presto.GCSToPrestoOperator`.

This operator assumes that CSV does not have headers and the data is corresponding to the columns in a
pre-existing presto table. Optionally, you can provide schema as tuple/list of strings or as a path to a
JSON file in the same bucket as the CSV file.

.. exampleinclude:: /../../presto/tests/system/presto/example_gcs_to_presto.py
    :language: python
    :dedent: 4
    :start-after: [START gcs_csv_to_presto_table]
    :end-before: [END gcs_csv_to_presto_table]
