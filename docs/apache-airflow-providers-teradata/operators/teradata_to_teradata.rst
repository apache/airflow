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

.. _howto/operator:TeradataToTeradataOperator:

TeradataToTeradataOperator
==========================

The purpose of TeradataToTeradataOperator is to define tasks involving data transfer between two Teradata instances.
Use the :class:`TeradataToTeradataOperator <airflow.providers.teradata.transfers.teradata_to_teradata>`
to transfer data between two Teradata instances.

Transfer data between two Teradata instances
-----------------------------------------------

To transfer data between two Teradata instances, use the
:class:`~airflow.providers.teradata.transfers.teradata_to_teradata.TeradataToTeradataOperator`.

An example usage of the TeradataToTeradataOperator is as follows:

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata_to_teradata_transfer.py
    :language: python
    :start-after: [START teradata_to_teradata_transfer_operator_howto_guide_transfer_data]
    :end-before: [END teradata_to_teradata_transfer_operator_howto_guide_transfer_data]

The complete TeradataToTeradata Transfer Operator DAG
-----------------------------------------------------

When we put everything together, our DAG should look like this:

.. exampleinclude:: /../../tests/system/providers/teradata/example_teradata.py
    :language: python
    :start-after: [START teradata_operator_howto_guide]
    :end-before: [END teradata_operator_howto_guide]
