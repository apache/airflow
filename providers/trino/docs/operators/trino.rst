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

.. _howto/operator:TrinoOperator:

TrinoOperator
=============

Use the :class:`TrinoOperator <airflow.providers.trino.operators.trino>` to execute
SQL commands in a `Trino <https://trino.io/>`__ query engine.


Using the Operator
^^^^^^^^^^^^^^^^^^

Use the ``trino_conn_id`` argument to connect to your Trino instance

An example usage of the TrinoOperator is as follows:

.. exampleinclude:: /../../tests/system/providers/trino/example_trino.py
    :language: python
    :start-after: [START howto_operator_trino]
    :end-before: [END howto_operator_trino]

.. note::

  This Operator can be used to run any syntactically correct Trino query, and multiple queries can be
  passed either using a ``list`` or a ``string``
