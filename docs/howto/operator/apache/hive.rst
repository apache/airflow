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
==========================

`Apache Hive <https://hive.apache.org/>`__ is an open source data warehouse software facilitates reading, writing, and managing large datasets residing in distributed storage using SQL. Structure can be projected onto data already in storage. A command line tool and JDBC driver are provided to connect users to Hive.

.. contents::
  :depth: 1
  :local:

Prerequisite
------------

To use operators, you must configure a :doc:`Cassandra Connection <../../connection/hive>`.

.. _howto/operator:HiveOperator:

Executes hql code or hive script in a specific Hive database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hive.operators.hive.HiveOperator` operator is used to execute hql code or hive script in a specific Hive database.

Use the ``hql`` the hql to be executed. Note that you may also use a relative path from the dag file of a (template) hive script. (templated).


.. exampleinclude:: /../airflow/providers/apache/hive/example_dags/example_twitter_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_hive_hive_operator]
    :end-before: [END howto_operator_hive_hive_operator]
    

.. _howto/operator:HiveStatsCollectionOperator:

Gathers partition statistics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hive.operators.hive_stats.HiveStatsCollectionOperator` operator is used to Gathers partition statistics using a dynamically generated Presto query, inserts the stats into a MySql table with this format. Stats overwrite themselves if you rerun the same date/partition. 

Use the ``table`` the source table, in the format ``database.table_name``. (templated)

Use the ``partition`` the source partition.

.. exampleinclude:: /../airflow/providers/apache/hive/example_dags/example_twitter_dag.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_hive_stats_collection_operator]
    :end-before: [END howto_operator_hive_stats_collection_operator]



.. _howto/operator:HiveToMySqlOperator:

Moves data from Hive to MySQL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hive.transfers.hive_to_mysql.HiveToMySqlOperator` operator is used to move data from Hive to MySQL, note that for now the data is loaded into memory before being pushed to MySQL, so this operator should be used for smallish amount of data.

Use the ``sql`` SQL query to execute against MySQL server.

Use the ``mysql_table`` target MySQL table, use dot notation to target a specific database.

.. exampleinclude:: /../airflow/providers/apache/hive/example_dags/
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_hive_hive_to_mysql_operator]
    :end-before: [END howto_operator_hive_hive_to_mysql_operator]
    


.. _howto/operator:HiveToSambaOperator:

Executes hql code in a specific Hive database and loads the results of the query as a csv to a Samba location.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hive.transfers.hive_to_samba.HiveToSambaOperator` operator is used to Execute hql code in a specific Hive database and loads the results of the query as a csv to a Samba location.

Use the ``hql`` the hql to be exported.

Use the ``destination_filepath`` the file path to where the file will be pushed onto samba

.. exampleinclude:: /../airflow/providers/apache/hive/example_dags/
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_hive_hive_to_samba_operator]
    :end-before: [END howto_operator_hive_hive_to_samba_operator]


.. _howto/operator:MsSqlToHiveOperator:

Moves data from Microsoft SQL Server to Hive
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hive.transfers.mssql_to_hive.MsSqlToHiveOperator` operator is used to Move data from Microsoft SQL Server to Hive

Use the ``sql`` SQL query to execute against the Microsoft SQL Server


.. exampleinclude:: /../airflow/providers/apache/hive/example_dags/
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_hive_mssql_to_hive_operator]
    :end-before: [END howto_operator_hive_hive_mssql_to_hive_operator]
    
    
.. _howto/operator:MySqlToHiveOperator:

Moves data from MySql to Hive
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hive.transfers.mysql_to_hive.MySqlToHiveOperator` operator is used to Move data from MySql to Hive

Use the ``sql`` SQL query to execute against the MySQL database
Use the ``hive_table`` target Hive table, use dot notation to target a specific database.

.. exampleinclude:: /../airflow/providers/apache/hive/example_dags/
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_hive_mysql_to_hive_operator]
    :end-before: [END howto_operator_hive_hive_mysql_to_hive_operator]
    

 .. _howto/operator:S3ToHiveOperator:

Moves data from S3 to Hive
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hive.transfers.s3_to_hive.S3ToHiveOperator` operator is used to Move data from S3 to Hive

Use the ``s3_key`` The key to be retrieved from S3

Use the ``field_dict`` A dictionary of the fields name in the file

Use the ``hive_table`` target Hive table, use dot notation to target a specific database.

.. exampleinclude:: /../airflow/providers/apache/hive/example_dags/
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_hive_s3_to_hive_operator]
    :end-before: [END howto_operator_hive_hive_s3_to_hive_operator]
    

 .. _howto/operator:VerticaToHiveOperator:

Moves data from Vertica to Hive
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.hive.transfers.vertica_to_hive.VerticaToHiveOperator` operator is used to Move data from Vertica to Hive

Use the ``sql`` SQL query to execute against Vertica database.

Use the ``hive_table`` target Hive table, use dot notation to target a specific database.

.. exampleinclude:: /../airflow/providers/apache/hive/example_dags/
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_hive_vertica_to_hive_operator]
    :end-before: [END howto_operator_hive_hive_vertica_to_hive_operator]


Reference
^^^^^^^^^

For further information, look at `Hive Query Language (HQL) <https://cwiki.apache.org/confluence/display/Hive/Home#Home-UserDocumentation>`_.
