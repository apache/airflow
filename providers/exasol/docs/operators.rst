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



.. _howto/operator:ExasolOperator:

SQLExecuteQueryOperator to connect to Exasol
====================================================

Use the :class:`SQLExecuteQueryOperator<airflow.providers.common.sql.operators.sql>` to execute
SQL commands in an `Exasol <https://www.exasol.com/>`__ database.

.. note::
    Previously, an ``ExasolOperator`` was used to perform this kind of operation.
    After deprecation this has been removed. Please use ``SQLExecuteQueryOperator`` instead.

.. note::
    Make sure you have installed the ``apache-airflow-providers-exasol`` package
    and its dependency ``pyexasol`` to enable Exasol support.

Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``conn_id`` argument to connect to your Exasol instance where
the connection metadata is structured as follows:

.. list-table:: Exasol Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - Host: string
     - Exasol hostname, container alias or IP address
   * - Schema: string
     - Default schema name (e.g. ``TEST``) (optional)
   * - Login: string
     - Exasol username (e.g. ``TEST`` or ``sys``)
   * - Password: string
     - Exasol password
   * - Port: int
     - Exasol port (default: 8563)
   * - Extra: JSON
     - Additional connection configuration passed to *pyexasol*, such as:
       ``{"encryption": false}`` or
       ``{"encryption": true, "websocket_sslopt": {"cert_reqs": 0}}``

An example usage of the SQLExecuteQueryOperator to connect to Exasol is as follows:

.. exampleinclude:: /../../exasol/tests/system/exasol/example_exasol.py
    :language: python
    :start-after: [START howto_operator_exasol]
    :end-before: [END howto_operator_exasol]

Reference
^^^^^^^^^
For further information, look at:

* `Exasol Documentation <https://docs.exasol.com/>`__

.. note::

  Parameters provided directly via SQLExecuteQueryOperator() take precedence
  over those specified in the Airflow connection metadata (such as ``schema``, ``login``, ``password``, etc).
