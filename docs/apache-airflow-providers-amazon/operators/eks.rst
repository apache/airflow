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


Amazon Elastic Kubernetes Service (EKS) Operators
=================================================

`Amazon Elastic Kubernetes Service (Amazon EKS) <https://aws.amazon.com/eks/>`__  is a managed service
that makes it easy for you to run Kubernetes on AWS without needing to stand up or maintain your own
Kubernetes control plane. Kubernetes is an open-source system for automating the deployment, scaling,
and management of containerized applications.

Airflow provides operators to create and interact with the EKS clusters and compute infrastructure.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
^^^^^^^^^^^^^^^^^^

.. include::/operators/_partials/prerequisite_tasks.rst


Manage Amazon EKS Clusters
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _howto/operator:EKSCreateClusterOperator:

Create an Amazon EKS Cluster
""""""""""""""""""""""""""""

To create an Amazon EKS Cluster you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSCreateClusterOperator`.

Note: An AWS IAM role with the following permissions is required:
  ``eks.amazonaws.com`` must be added to the Trusted Relationships
  ``AmazonEKSClusterPolicy`` IAM Policy must be attached

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_cluster.py
    :language: python
    :start-after: [START howto_operator_eks_create_cluster]
    :end-before: [END howto_operator_eks_create_cluster]

.. _howto/operator:EKSDescribeClusterOperator:

Describe an Amazon EKS Cluster
""""""""""""""""""""""""""""""

To get details of an existing Amazon EKS Cluster you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSDescribeClusterOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_cluster.py
    :language: python
    :start-after: [START howto_operator_eks_describe_cluster]
    :end-before: [END howto_operator_eks_describe_cluster]

.. _howto/operator:EKSDescribeAllClustersOperator:

Describe all Amazon EKS Clusters
""""""""""""""""""""""""""""""""

To get details of all existing Amazon EKS Cluster you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSDescribeAllClustersOperator`.

.. _howto/operator:EKSListClustersOperator:

List all Amazon EKS Clusters
""""""""""""""""""""""""""""

To get a list of the names of existing Amazon EKS Clusters you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSListClustersOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_cluster.py
    :language: python
    :start-after: [START howto_operator_eks_list_clusters]
    :end-before: [END howto_operator_eks_list_clusters]

.. _howto/operator:EKSDeleteClusterOperator:

Delete an Amazon EKS Cluster
""""""""""""""""""""""""""""

To delete an existing Amazon EKS Cluster you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSDeleteClusterOperator`.

Note: If the cluster has any attached nodegroups, those will be deleted as well.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_cluster.py
    :language: python
    :start-after: [START howto_operator_eks_delete_cluster]
    :end-before: [END howto_operator_eks_delete_cluster]

Manage Amazon EKS Managed Nodegroups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _howto/operator:EKSCreateNodegroupOperator:

Create an Amazon EKS Managed NodeGroup
""""""""""""""""""""""""""""""""""""""

To create an Amazon EKS Managed Nodegroup you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSCreateNodegroupOperator`.

Note:  An AWS IAM role with the following permissions is required:
  ``ec2.amazon.aws.com`` must be in the Trusted Relationships
  ``AmazonEC2ContainerRegistryReadOnly`` IAM Policy must be attached
  ``AmazonEKSWorkerNodePolicy`` IAM Policy must be attached

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_nodegroup.py
    :language: python
    :start-after: [START howto_operator_eks_create_nodegroup]
    :end-before: [END howto_operator_eks_create_nodegroup]

.. _howto/operator:EKSDescribeNodegroupOperator:

Describe an Amazon EKS Managed Nodegroup
""""""""""""""""""""""""""""""""""""""""

To get details of an existing Amazon EKS Managed Nodegroup you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSDescribeNodegroupOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_nodegroup.py
    :language: python
    :start-after: [START howto_operator_eks_describe_nodegroup]
    :end-before: [END howto_operator_eks_describe_nodegroup]

.. _howto/operator:EKSDescribeAllNodegroupsOperator:

Describe all Amazon EKS Managed Nodegroups
""""""""""""""""""""""""""""""""""""""""""

To get details of all existing Amazon EKS Managed Nodegroups on a provided cluster you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSDescribeAllNodegroupsOperator`.

.. _howto/operator:EKSListNodegroupsOperator:

List all Amazon EKS Managed Nodegroups
""""""""""""""""""""""""""""""""""""""""""

To get a list of the names of existing Amazon EKS Managed Nodegroups on a provided cluster you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSListNodegroupsOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_nodegroup.py
    :language: python
    :start-after: [START howto_operator_eks_list_nodegroup]
    :end-before: [END howto_operator_eks_list_nodegroup]

.. _howto/operator:EKSDeleteNodegroupOperator:

Delete an Amazon EKS Managed Nodegroup
""""""""""""""""""""""""""""""""""""""

To delete an existing Amazon EKS Managed Nodegroup you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSDeleteNodegroupOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_nodegroup.py
    :language: python
    :start-after: [START howto_operator_eks_delete_nodegroup]
    :end-before: [END howto_operator_eks_delete_nodegroup]


Create an Amazon EKS Cluster and Nodegroup in one step
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create an Amazon EKS Cluster and an EKS Managed Nodegroup in one command, you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSCreateClusterOperator`.

Note: An AWS IAM role with the following permissions is required:
  ``ec2.amazon.aws.com`` must be in the Trusted Relationships
  ``eks.amazonaws.com`` must be added to the Trusted Relationships
  ``AmazonEC2ContainerRegistryReadOnly`` IAM Policy must be attached
  ``AmazonEKSClusterPolicy`` IAM Policy must be attached
  ``AmazonEKSWorkerNodePolicy`` IAM Policy must be attached

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_cluster_with_nodegroup.py
    :language: python
    :start-after: [START howto_operator_eks_create_cluster_with_compute]
    :end-before: [END howto_operator_eks_create_cluster_with_compute]

.. _howto/operator:EKSPodOperator:

Perform a Task on an Amazon EKS Cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To run a pod on an existing Amazon EKS Cluster, you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EKSPodOperator`.

Note: An Amazon EKS Cluster with underlying compute infrastructure is required.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_pod_operation.py
    :language: python
    :start-after: [START howto_operator_eks_pod_operator]
    :end-before: [END howto_operator_eks_pod_operator]

Reference
---------

For further information, look at:

* `Boto3 Library Documentation for EKS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/eks.html>`__
