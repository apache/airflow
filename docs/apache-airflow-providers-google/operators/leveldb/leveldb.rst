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



.. _howto/operator:LevelDBOperator:

Google LevelDB Operator
================================

`LevelDB <https://github.com/google/leveldb>`__ is a fast key-value storage library written at Google that provides
an ordered mapping from string keys to string values.

.. note::

    To use LevelDB hooks and operators you must requires installation of ``plyvel``.  It will be
    installed if you specify the extra ``apache-airflow-providers-google[leveldb]``.

Put key
^^^^^^^^^^^^^^^

Get, put, delete key or write_batch, create database with comparator or different options in LevelDB is performed with the
:class:`~airflow.providers.google.leveldb.operators.leveldb.LevelDBOperator` operator.

.. exampleinclude:: /../../providers/tests/system/google/leveldb/example_leveldb.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_leveldb_put_key]
    :end-before: [END howto_operator_leveldb_put_key]


Reference
^^^^^^^^^

For further information, look at:

* `Product Documentation <https://github.com/google/leveldb/blob/main/doc/index.md>`__
* `Client Library Documentation <https://plyvel.readthedocs.io/en/latest/>`__
