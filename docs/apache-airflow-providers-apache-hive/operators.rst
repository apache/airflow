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

The Apache Hive data warehouse software facilitates reading, writing,
and managing large datasets residing in distributed storage using SQL.
Structure can be projected onto data already in storage.

HiveOperator
------------

This operator executes hql code or hive script in a specific Hive database.

.. exampleinclude:: /../../providers/tests/system/apache/hive/example_twitter_dag.py
    :language: python
    :dedent: 4
    :start-after: [START create_hive]
    :end-before: [END create_hive]


Reference
^^^^^^^^^

For more information check `Apache Hive documentation <https://hive.apache.org/>`__.
