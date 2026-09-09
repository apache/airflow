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

.. _howto/hook:AdbcHook:

Using AdbcHook
==============

Use :class:`~airflow.providers.apache.arrow.hooks.adbc.AdbcHook` to interact with any database
that has an `ADBC <https://arrow.apache.org/adbc/>`__ driver.  The hook extends
:class:`~airflow.providers.common.sql.hooks.sql.DbApiHook`, so it works transparently with
:class:`~airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator` and the other
operators from the ``common.sql`` provider — just pass ``conn_id`` pointing at an ``adbc``
connection.

For direct hook usage (e.g. inside a ``@dag.task`` function), the example below demonstrates
creating a table, bulk-inserting rows via Arrow-native transfer, querying the results, and
cleaning up — all against a SQLite ADBC connection.

.. exampleinclude:: /../../arrow/tests/system/apache/arrow/example_adbc.py
    :language: python
    :start-after: [START howto_adbc_hook]
    :end-before: [END howto_adbc_hook]

See :ref:`Connection types <howto/connection:adbc>` for how to configure the ``adbc`` connection.
