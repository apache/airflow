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



Google LevelDB Operators
================================

`LevelDB <https://github.com/google/leveldb>`__ is a fast key-value storage library written at Google that provides
an ordered mapping from string keys to string values.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst


.. _howto/operator:LevelDBOperator:

Get, put, delete key or write_batch
^^^^^^^^^^^^^^^

Get, put, delete key or write_batch, create database with comparator or different options in LevelDB is performed with the
:class:`~airflow.providers.google.leveldb.operators.leveldb.LevelDBOperator` operator.

.. exampleinclude:: /../../airflow/providers/google/leveldb/example_dags/example_leveldb.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_leveldb_put_key]
    :end-before: [END howto_operator_leveldb_put_key]

You can use :ref:`Jinja templating <jinja-templating>` with
:template-fields:`airflow.providers.google.leveldb.operators.leveldb.LevelDBOperator`
parameters which allows you to dynamically determine values.


Reference
^^^^^^^^^

For further information, look at:

* `Product Documentation <https://github.com/google/leveldb/blob/master/doc/index.md>`__
* `Client Library Documentation <https://plyvel.readthedocs.io/en/latest/>`__
