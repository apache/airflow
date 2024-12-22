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

Google Cloud AlloyDB Operators
===============================

The `AlloyDB for PostgreSQL <https://cloud.google.com/alloydb/docs/overview>`__
is a fully managed, PostgreSQL-compatible database service that's designed for your most demanding workloads,
including hybrid transactional and analytical processing. AlloyDB pairs a Google-built database engine with a
cloud-based, multi-node architecture to deliver enterprise-grade performance, reliability, and availability.

Airflow provides operators to manage AlloyDB clusters.

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include:: /operators/_partials/prerequisite_tasks.rst

.. _howto/operator:AlloyDBCreateClusterOperator:

Create cluster
""""""""""""""

To create an AlloyDB cluster (primary end secondary) you can use
:class:`~airflow.providers.google.cloud.operators.alloy_db.AlloyDBCreateClusterOperator`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/alloy_db/example_alloy_db.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_alloy_db_create_cluster]
    :end-before: [END howto_operator_alloy_db_create_cluster]

.. _howto/operator:AlloyDBUpdateClusterOperator:

Update cluster
""""""""""""""

To update an AlloyDB cluster you can use
:class:`~airflow.providers.google.cloud.operators.alloy_db.AlloyDBUpdateClusterOperator`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/alloy_db/example_alloy_db.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_alloy_db_update_cluster]
    :end-before: [END howto_operator_alloy_db_update_cluster]

.. _howto/operator:AlloyDBDeleteClusterOperator:

Delete cluster
""""""""""""""

To delete an AlloyDB cluster you can use
:class:`~airflow.providers.google.cloud.operators.alloy_db.AlloyDBDeleteClusterOperator`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/alloy_db/example_alloy_db.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_alloy_db_delete_cluster]
    :end-before: [END howto_operator_alloy_db_delete_cluster]
