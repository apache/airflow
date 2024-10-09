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


Apache Pinot Hooks
==================


`Apache Pinot <https://pinot.apache.org/>`__ is a column-oriented, open-source, distributed data store written in Java. Pinot is designed to execute OLAP queries with low latency. It is suited in contexts where fast analytics, such as aggregations, are needed on immutable data, possibly, with real-time data ingestion.


Prerequisite
------------

.. To use Pinot hooks, you must configure :doc:`Pinot Connection <connections/pinot>`.

.. _howto/operator:PinotHooks:

PinotAdminHook
--------------

This hook is a wrapper around the pinot-admin.sh script, which is used for administering a Pinot cluster and provided by Apache Pinot distribution. For now, only small subset of its subcommands are implemented, which are required to ingest offline data into Apache Pinot (i.e., AddSchema, AddTable, CreateSegment, and UploadSegment). Their command options are based on Pinot v0.1.0.

Parameters
----------

For parameter definition, take a look at :class:`~airflow.providers.apache.pinot.hooks.pinot.PinotAdminHook`

.. exampleinclude:: /../../providers/tests/system/apache/pinot/example_pinot_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_pinot_admin_hook]
    :end-before: [END howto_operator_pinot_admin_hook]

Reference
^^^^^^^^^

For more information, please see the documentation at ``Apache Pinot improvements for PinotAdminHook<https://pinot.apache.org/>``

PinotDbApiHook
--------------

This hook uses standard-SQL endpoint since PQL endpoint is soon to be deprecated.

Parameters
----------

For parameter definition, take a look at :class:`~airflow.providers.apache.pinot.hooks.pinot.PinotDbApiHook`

.. exampleinclude:: /../../providers/tests/system/apache/pinot/example_pinot_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_pinot_dbapi_example]
    :end-before: [END howto_operator_pinot_dbapi_example]

Reference
^^^^^^^^^

For more information, please see the documentation at ``Pinot documentation on querying data <https://docs.pinot.apache.org/users/api/querying-pinot-using-standard-sql>``
