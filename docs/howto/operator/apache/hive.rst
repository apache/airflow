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



Apache Hive Operators
=====================

`Apache Hive <https://hive.apache.org/>`__ is an open source data warehouse software facilitates reading, writing, and managing large datasets residing in distributed storage using SQL. Structure can be projected onto data already in storage. A command line tool and JDBC driver are provided to connect users to Hive.

.. contents::
  :depth: 1
  :local:

Prerequisite
------------

To use operators, you must configure a :doc:`Hive Connection <../../connection/hive>`.

.. _howto/operator:HiveOperator:

HiveOperator
------------
Executes hive script in a specific Hive database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hive.operators.hive.HiveOperator` operator is used to execute hql code or hive script in a specific Hive database.

Use the ``hql`` for the hql script to be executed. Note that you may also use a relative path from the dag file of a (template) hive script. (templated).

.. exampleinclude:: /../airflow/providers/apache/hive/example_dags/example_twitter_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_hive_hive_operator]
    :end-before: [END howto_operator_hive_hive_operator]

.. _howto/operator:HiveStatsCollectionOperator:

HiveStatsCollectionOperator
---------------------------
Gathers partition statistics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hive.operators.hive_stats.HiveStatsCollectionOperator` operator is used to Gathers partition statistics using a dynamically generated Presto query, inserts the stats into a MySql table with this format. Stats overwrite themselves if you rerun the same date/partition. 

Use the ``table`` parameter for specifying the source table, in the format ``database.table_name``. (templated)

Use the ``partition`` parameter for specifying the source partition.

Reference
^^^^^^^^^

For further information, look at `Hive Query Language (HQL) <https://cwiki.apache.org/confluence/display/Hive/Home#Home-UserDocumentation>`_.
