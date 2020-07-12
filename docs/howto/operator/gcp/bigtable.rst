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



Google Cloud Bigtable Operators
===============================

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
------------------

.. include:: /howto/operator/gcp/_partials/prerequisite_tasks.rst


.. _howto/operator:BigtableCreateInstanceOperator:

BigtableCreateInstanceOperator
------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.bigtable.BigtableCreateInstanceOperator`
to create a Google Cloud Bigtable instance.

If the Cloud Bigtable instance with the given ID exists, the operator does not compare its configuration
and immediately succeeds. No changes are made to the existing instance.

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigtable.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_instance_create]
    :end-before: [END howto_operator_gcp_bigtable_instance_create]

.. _howto/operator:BigtableDeleteInstanceOperator:

BigtableDeleteInstanceOperator
------------------------------

Use the :class:`~airflow.providers.google.cloud.operators.bigtable.BigtableDeleteInstanceOperator`
to delete a Google Cloud Bigtable instance.

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigtable.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_instance_delete]
    :end-before: [END howto_operator_gcp_bigtable_instance_delete]

.. _howto/operator:BigtableUpdateClusterOperator:

BigtableUpdateClusterOperator
-----------------------------

Use the :class:`~airflow.providers.google.cloud.operators.bigtable.BigtableUpdateClusterOperator`
to modify number of nodes in a Cloud Bigtable cluster.

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigtable.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_cluster_update]
    :end-before: [END howto_operator_gcp_bigtable_cluster_update]

.. _howto/operator:BigtableCreateTableOperator:

BigtableCreateTableOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Creates a table in a Cloud Bigtable instance.

If the table with given ID exists in the Cloud Bigtable instance, the operator compares the Column Families.
If the Column Families are identical operator succeeds. Otherwise, the operator fails with the appropriate
error message.


Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigtable.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_table_create]
    :end-before: [END howto_operator_gcp_bigtable_table_create]

Advanced
""""""""

When creating a table, you can specify the optional ``initial_split_keys`` and ``column_families``.
Please refer to the Python Client for Google Cloud Bigtable documentation
`for Table <https://googleapis.github.io/google-cloud-python/latest/bigtable/table.html>`_ and `for Column
Families <https://googleapis.github.io/google-cloud-python/latest/bigtable/column-family.html>`_.

.. _howto/operator:BigtableDeleteTableOperator:

BigtableDeleteTableOperator
---------------------------

Use the :class:`~airflow.providers.google.cloud.operators.bigtable.BigtableDeleteTableOperator`
to delete a table in Google Cloud Bigtable.

Using the operator
""""""""""""""""""

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigtable.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_table_delete]
    :end-before: [END howto_operator_gcp_bigtable_table_delete]

.. _howto/operator:BigtableTableReplicationCompletedSensor:

BigtableTableReplicationCompletedSensor
---------------------------------------

You can create the operator with or without project id. If project id is missing
it will be retrieved from the GCP connection used. Both variants are shown:

Use the :class:`~airflow.providers.google.cloud.sensors.bigtable.BigtableTableReplicationCompletedSensor`
to wait for the table to replicate fully.

The same arguments apply to this sensor as the BigtableCreateTableOperator.

**Note:** If the table or the Cloud Bigtable instance does not exist, this sensor waits for the table until
timeout hits and does not raise any exception.

Using the operator
""""""""""""""""""

.. exampleinclude:: ../../../../airflow/providers/google/cloud/example_dags/example_bigtable.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_gcp_bigtable_table_wait_for_replication]
    :end-before: [END howto_operator_gcp_bigtable_table_wait_for_replication]

Reference
---------

For further information, look at:

* `Client Library Documentation <https://googleapis.github.io/google-cloud-python/latest/bigtable/index.html>`__
* `Product Documentation <https://cloud.google.com/bigtable/docs/>`__
