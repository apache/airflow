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

Apache Sqoop is a tool designed to transfer data between Hadoop and relational databases.
You can use Sqoop to import data from a relational database management system (RDBMS) such as MySQL
or Oracle into the Hadoop Distributed File System (HDFS), transform the data in Hadoop MapReduce,
and then export the data back into an RDBMS.

SqoopOperator
-------------

Execute a Sqoop Job
^^^^^^^^^^^^^^^^^^^

The :class:`~airflow.providers.apache.sqoop.operators.sqoop.SqoopOperator` operator is used to
execute a sqoop job.

Reference
^^^^^^^^^

For further information, look at `Sqoop User Guide  <https://sqoop.apache.org/docs/1.4.2/SqoopUserGuide.html>`_.
