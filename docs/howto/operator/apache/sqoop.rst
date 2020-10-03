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



Apache Sqoop Operators
======================

`Apache Sqoop (TM) <https://sqoop.apache.org/>`__ is a tool designed to transfer data between Hadoop and relational databases or mainframes. You can use Sqoop to import data from a relational database management system (RDBMS) such as MySQL or Oracle or a mainframe into the Hadoop Distributed File System (HDFS), transform the data in Hadoop MapReduce, and then export the data back into an RDBMS.

Sqoop automates most of this process, relying on the database to describe the schema for the data to be imported. Sqoop uses MapReduce to import and export the data, which provides parallel operation as well as fault tolerance.


.. contents::
  :depth: 1
  :local:

Prerequisite
------------

To use operators, you must configure a :doc:`Cassandra Connection <../../connection/sqoop>`.

.. _howto/operator:SqoopOperator:

SqoopOperator
-------------
Execute a Sqoop job
^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.sqoop.operators.sqoop.SqoopOperator` operator is used to execute a sqoop job.

Use the ``query`` parameter to import result of arbitrary SQL query.

Reference
^^^^^^^^^

For further information, look at `Sqoop User Guide (v1.4.6) <https://sqoop.apache.org/docs/1.4.6/SqoopUserGuide.html>`_.
