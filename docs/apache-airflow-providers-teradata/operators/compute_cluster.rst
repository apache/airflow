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


.. _howto/operator:TeradataComputeClusterProvisionOperator:


=======================================
TeradataComputeClusterProvisionOperator
=======================================

The purpose of ``TeradataComputeClusterProvisionOperator`` is to provision the new Teradata Vantage Cloud Lake
Compute Cluster with specified Compute Group Name and Compute Profile Name.
Use the :class:`TeradataComputeClusterProvisionOperator <airflow.providers.teradata.operators.teradata_compute_cluster>`
to provision the new Compute Cluster in Teradata Vantage Cloud Lake.



An example usage of the TeradataComputeClusterProvisionOperator to provision the new Compute Cluster in
Teradata Vantage Cloud Lake is as follows:

.. exampleinclude:: /../../providers/tests/system/teradata/example_teradata_compute_cluster.py
    :language: python
    :start-after: [START teradata_vantage_lake_compute_cluster_provision_howto_guide]
    :end-before: [END teradata_vantage_lake_compute_cluster_provision_howto_guide]


.. _howto/operator:TeradataComputeClusterDecommissionOperator:


==========================================
TeradataComputeClusterDecommissionOperator
==========================================

The purpose of ``TeradataComputeClusterDecommissionOperator`` is to decommission the specified Teradata Vantage Cloud Lake
Compute Cluster.
Use the :class:`TeradataComputeClusterProvisionOperator <airflow.providers.teradata.operators.teradata_compute_cluster>`
to decommission the specified Teradata Vantage Cloud Lake Compute Cluster.



An example usage of the TeradataComputeClusterDecommissionOperator to decommission the specified Teradata Vantage Cloud
Lake Compute Cluster is as follows:

.. exampleinclude:: /../../providers/tests/system/teradata/example_teradata_compute_cluster.py
    :language: python
    :start-after: [START teradata_vantage_lake_compute_cluster_decommission_howto_guide]
    :end-before: [END teradata_vantage_lake_compute_cluster_decommission_howto_guide]


.. _howto/operator:TeradataComputeClusterResumeOperator:


=====================================
TeradataComputeClusterResumeOperator
=====================================

The purpose of ``TeradataComputeClusterResumeOperator`` is to start the Teradata Vantage Cloud Lake
Compute Cluster of specified Compute Group Name and Compute Profile Name.
Use the :class:`TeradataComputeClusterResumeOperator <airflow.providers.teradata.operators.teradata_compute_cluster>`
to start the specified Compute Cluster in Teradata Vantage Cloud Lake.



An example usage of the TeradataComputeClusterSuspendOperator to start the specified Compute Cluster in
Teradata Vantage Cloud Lake is as follows:

.. exampleinclude:: /../../providers/tests/system/teradata/example_teradata_compute_cluster.py
    :language: python
    :start-after: [START teradata_vantage_lake_compute_cluster_resume_howto_guide]
    :end-before: [END teradata_vantage_lake_compute_cluster_resume_howto_guide]

.. _howto/operator:TeradataComputeClusterSuspendOperator:


=====================================
TeradataComputeClusterSuspendOperator
=====================================

The purpose of ``TeradataComputeClusterSuspendOperator`` is to suspend the Teradata Vantage Cloud Lake
Compute Cluster of specified Compute Group Name and Compute Profile Name.
Use the :class:`TeradataComputeClusterSuspendOperator <airflow.providers.teradata.operators.teradata_compute_cluster>`
to suspend the specified Compute Cluster in Teradata Vantage Cloud Lake.



An example usage of the TeradataComputeClusterSuspendOperator to suspend the specified Compute Cluster in
Teradata Vantage Cloud Lake is as follows:

.. exampleinclude:: /../../providers/tests/system/teradata/example_teradata_compute_cluster.py
    :language: python
    :start-after: [START teradata_vantage_lake_compute_cluster_suspend_howto_guide]
    :end-before: [END teradata_vantage_lake_compute_cluster_suspend_howto_guide]
