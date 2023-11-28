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

.. _howto/operator:TeradataOperator:

TeradataOperator
================

The Teradata connection type provides connection to Teradata.

Execute SQL in an Teradata
---------------------------------

To execute arbitrary SQL in an Teradata, use the
:class:`~airflow.providers.teradata.operators.teradata.TeradataOperator`.

An example of executing a simple query is as follows:

.. exampleinclude:: /../../airflow/providers/teradata/example_dags/example_teradata_operator.py
    :language: python
    :start-after: [START howto_teradata_operator]
    :end-before: [END howto_teradata_operator]
