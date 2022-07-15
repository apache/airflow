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

OpenMLDB Operators
==================

Overview
--------

Airflow to OpenMLDB integration provides several operators to interact with OpenMLDB.

 - :class:`~airflow.providers.openmldb.operators.openmldb.OpenMLDBSQLOperator`
 - :class:`~airflow.providers.openmldb.operators.openmldb.OpenMLDBLoadDataOperator`
 - :class:`~airflow.providers.openmldb.operators.openmldb.OpenMLDBSelectIntoOperator`
 - :class:`~airflow.providers.openmldb.operators.openmldb.OpenMLDBDeployOperator`

Feature Extraction on OpenMLDB
------------------------------

Purpose
"""""""

This example dag uses ``OpenMLDBLoadDataOperator`` and ``OpenMLDBSelectIntoOperator`` to load data and
extract feature from OpenMLDB in offline mode.

Defining tasks
""""""""""""""

In the following code we create a new bucket and then delete the bucket.

.. exampleinclude:: /../../tests/system/providers/openmldb/example_openmldb.py
    :language: python
    :start-after: [START load_data_and_extract_feature_offline]
    :end-before: [END load_data_and_extract_feature_offline]
