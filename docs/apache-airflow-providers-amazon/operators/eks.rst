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

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Overview
--------

Airflow to Amazon Elastic Kubernetes Service (EKS) integration provides Operators to create and
interact with the EKS clusters and compute infrastructure.

 - :class:`~airflow.providers.amazon.aws.operators.eks`

4 example_dags are provided which showcase these operators in action.

 - ``example_eks_create_cluster.py``
 - ``example_eks_create_cluster_with_nodegroup.py``
 - ``example_eks_create_nodegroup.py``
 - ``example_eks_pod_operator.py``


.. _howto/operator:EKSCreateClusterOperator:

Creating Amazon EKS Clusters
----------------------------

Purpose
"""""""

This example dag ``example_eks_create_cluster.py`` uses ``EKSCreateClusterOperator`` to create an Amazon
EKS Cluster, ``EKSListClustersOperator`` and ``EKSDescribeClusterOperator`` to verify creation, then
``EKSDeleteClusterOperator`` to delete the Cluster.

Prerequisites
"""""""""""""

An AWS IAM role with the following permissions:

  "eks.amazonaws.com" must be added to the Trusted Relationships
  "AmazonEKSClusterPolicy" IAM Policy must be attached

Defining tasks
""""""""""""""

In the following code we create a new Amazon EKS Cluster.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_cluster.py
    :language: python
    :start-after: [START howto_operator_eks_create_cluster]
    :end-before: [END howto_operator_eks_create_cluster]


.. _howto/operator:EKSListClustersOperator:
.. _howto/operator:EKSDescribeClusterOperator:


Listing and Describing Amazon EKS Clusters
-------------------------------------------

Defining tasks
""""""""""""""

In the following code we list all Amazon EKS Clusters.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_cluster.py
    :language: python
    :start-after: [START howto_operator_eks_list_clusters]
    :end-before: [END howto_operator_eks_list_clusters]

In the following code we retrieve details for a given Amazon EKS Cluster.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_cluster.py
    :language: python
    :start-after: [START howto_operator_eks_describe_cluster]
    :end-before: [END howto_operator_eks_describe_cluster]


.. _howto/operator:EKSDeleteClusterOperator:

Deleting Amazon EKS Clusters
----------------------------

Defining tasks
""""""""""""""

In the following code we delete a given Amazon EKS Cluster.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_cluster.py
    :language: python
    :start-after: [START howto_operator_eks_delete_cluster]
    :end-before: [END howto_operator_eks_delete_cluster]


.. _howto/operator:EKSCreateNodegroupOperator:

Creating Amazon EKS Managed NodeGroups
--------------------------------------

Purpose
"""""""

This example dag ``example_eks_create_nodegroup.py`` uses ``EKSCreateNodegroupOperator``
to create an Amazon EKS Managed Nodegroup using an existing cluster, ``EKSListNodegroupsOperator``
and ``EKSDescribeNodegroupOperator`` to verify creation, then ``EKSDeleteNodegroupOperator``
to delete the nodegroup.

Prerequisites
"""""""""""""

An AWS IAM role with the following permissions:

  "ec2.amazon.aws.com" must be in the Trusted Relationships
  "AmazonEC2ContainerRegistryReadOnly" IAM Policy must be attached
  "AmazonEKSWorkerNodePolicy" IAM Policy must be attached

Defining tasks
""""""""""""""

In the following code we create a new Amazon EKS Managed Nodegroup.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_nodegroup.py
    :language: python
    :start-after: [START howto_operator_eks_create_nodegroup]
    :end-before: [END howto_operator_eks_create_nodegroup]


.. _howto/operator:EKSListNodegroupsOperator:
.. _howto/operator:EKSDescribeNodegroupOperator:

Listing and Describing Amazon EKS Clusters
-------------------------------------------

Defining tasks
""""""""""""""

In the following code we retrieve details for a given Amazon EKS nodegroup.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_nodegroup.py
    :language: python
    :start-after: [START howto_operator_eks_describe_nodegroup]
    :end-before: [END howto_operator_eks_describe_nodegroup]


In the following code we list all Amazon EKS Nodegroups in a given EKS Cluster.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_nodegroup.py
    :language: python
    :start-after: [START howto_operator_eks_list_nodegroup]
    :end-before: [END howto_operator_eks_list_nodegroup]


.. _howto/operator:EKSDeleteNodegroupOperator:

Deleting Amazon EKS Managed Nodegroups
--------------------------------------

Defining tasks
""""""""""""""

In the following code we delete an Amazon EKS nodegroup.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_nodegroup.py
    :language: python
    :start-after: [START howto_operator_eks_delete_nodegroup]
    :end-before: [END howto_operator_eks_delete_nodegroup]


Creating Amazon EKS Clusters and Node Groups Together
------------------------------------------------------

Purpose
"""""""

This example dag ``example_eks_create_stack.py`` demonstrates using
``EKSCreateClusterOperator`` to create an Amazon EKS cluster and underlying
Amazon EKS node group in one command.  ``EKSDescribeClustersOperator`` and
``EKSDescribeNodegroupsOperator`` verify creation, then ``EKSDeleteClusterOperator``
deletes all created resources.

Prerequisites
"""""""""""""

  ``ec2.amazon.aws.com`` must be in the Trusted Relationships
  ``eks.amazonaws.com`` must be added to the Trusted Relationships
  ``AmazonEC2ContainerRegistryReadOnly`` IAM Policy must be attached
  ``AmazonEKSClusterPolicy`` IAM Policy must be attached
  ``AmazonEKSWorkerNodePolicy`` IAM Policy must be attached

Defining tasks
""""""""""""""

In the following code we create a new Amazon EKS cluster and node group, verify creation,
then delete both resources.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_create_cluster_with_nodegroup.py
    :language: python
    :start-after: [START howto_operator_eks_create_cluster_with_compute]
    :end-before: [END howto_operator_eks_create_cluster_with_compute]


.. _howto/operator:EKSPodOperator:

Perform a Task on an Amazon EKS Cluster
---------------------------------------

Purpose
"""""""

This example dag ``example_eks_pod_operator.py`` demonstrates using
``EKSStartPodOperator`` to perform a command on Amazon EKS cluster.

Prerequisites
"""""""""""""

  1. An Amazon EKS Cluster with underlying compute infrastructure.
  2. The AWS CLI version 2 must be installed on the worker.

  see: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html

Defining tasks
""""""""""""""

In the following code we execute a command on an existing Amazon EKS cluster.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_pod_operation.py
    :language: python
    :start-after: [START howto_operator_eks_pod_operator]
    :end-before: [END howto_operator_eks_pod_operator]

Reference
---------

For further information, look at:

* `Boto3 Library Documentation for EKS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/eks.html>`__
