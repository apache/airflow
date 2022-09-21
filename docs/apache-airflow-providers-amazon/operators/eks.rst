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

=======================================
Amazon Elastic Kubernetes Service (EKS)
=======================================

`Amazon Elastic Kubernetes Service (Amazon EKS) <https://aws.amazon.com/eks/>`__  is a managed service
that makes it easy for you to run Kubernetes on AWS without needing to stand up or maintain your own
Kubernetes control plane. Kubernetes is an open-source system for automating the deployment, scaling,
and management of containerized applications.

Airflow provides operators to create and interact with the EKS clusters and compute infrastructure.

Prerequisite Tasks
------------------

.. include:: _partials/prerequisite_tasks.rst

Operators
---------

.. _howto/operator:EksCreateClusterOperator:

Create an Amazon EKS cluster
============================

To create an Amazon EKS Cluster you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EksCreateClusterOperator`.

Note: An AWS IAM role with the following permissions is required:
  ``eks.amazonaws.com`` must be added to the Trusted Relationships
  ``AmazonEKSClusterPolicy`` IAM Policy must be attached

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_eks_with_nodegroups.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eks_create_cluster]
    :end-before: [END howto_operator_eks_create_cluster]

Create an Amazon EKS cluster and node group in one step
=======================================================

To create an Amazon EKS cluster and an EKS managed node group in one command, you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EksCreateClusterOperator`.

Note: An AWS IAM role with the following permissions is required:
  ``ec2.amazon.aws.com`` must be in the Trusted Relationships
  ``eks.amazonaws.com`` must be added to the Trusted Relationships
  ``AmazonEC2ContainerRegistryReadOnly`` IAM Policy must be attached
  ``AmazonEKSClusterPolicy`` IAM Policy must be attached
  ``AmazonEKSWorkerNodePolicy`` IAM Policy must be attached

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_eks_with_nodegroup_in_one_step.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eks_create_cluster_with_nodegroup]
    :end-before: [END howto_operator_eks_create_cluster_with_nodegroup]

Create an Amazon EKS cluster and AWS Fargate profile in one step
================================================================

To create an Amazon EKS cluster and an AWS Fargate profile in one command, you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EksCreateClusterOperator`.

Note: An AWS IAM role with the following permissions is required:
  ``ec2.amazon.aws.com`` must be in the Trusted Relationships
  ``eks.amazonaws.com`` must be added to the Trusted Relationships
  ``AmazonEC2ContainerRegistryReadOnly`` IAM Policy must be attached
  ``AmazonEKSClusterPolicy`` IAM Policy must be attached
  ``AmazonEKSWorkerNodePolicy`` IAM Policy must be attached

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_with_fargate_in_one_step.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eks_create_cluster_with_fargate_profile]
    :end-before: [END howto_operator_eks_create_cluster_with_fargate_profile]

.. _howto/operator:EksDeleteClusterOperator:

Delete an Amazon EKS Cluster
============================

To delete an existing Amazon EKS Cluster you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EksDeleteClusterOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_eks_with_nodegroups.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eks_delete_cluster]
    :end-before: [END howto_operator_eks_delete_cluster]

Note: If the cluster has any attached resources, such as an Amazon EKS Nodegroup or AWS
  Fargate profile, the cluster can not be deleted.  Using the ``force`` parameter will
  attempt to delete any attached resources first.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_eks_with_nodegroup_in_one_step.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eks_force_delete_cluster]
    :end-before: [END howto_operator_eks_force_delete_cluster]

.. _howto/operator:EksCreateNodegroupOperator:

Create an Amazon EKS managed node group
=======================================

To create an Amazon EKS managed node group you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EksCreateNodegroupOperator`.

Note:  An AWS IAM role with the following permissions is required:
  ``ec2.amazon.aws.com`` must be in the Trusted Relationships
  ``AmazonEC2ContainerRegistryReadOnly`` IAM Policy must be attached
  ``AmazonEKSWorkerNodePolicy`` IAM Policy must be attached

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_eks_with_nodegroups.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eks_create_nodegroup]
    :end-before: [END howto_operator_eks_create_nodegroup]

.. _howto/operator:EksDeleteNodegroupOperator:

Delete an Amazon EKS managed node group
=======================================

To delete an existing Amazon EKS managed node group you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EksDeleteNodegroupOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_eks_with_nodegroups.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eks_delete_nodegroup]
    :end-before: [END howto_operator_eks_delete_nodegroup]

.. _howto/operator:EksCreateFargateProfileOperator:

Create an AWS Fargate Profile
=============================

To create an AWS Fargate Profile you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EksCreateFargateProfileOperator`.

Note:  An AWS IAM role with the following permissions is required:
  ``ec2.amazon.aws.com`` must be in the Trusted Relationships
  ``AmazonEC2ContainerRegistryReadOnly`` IAM Policy must be attached
  ``AmazonEKSWorkerNodePolicy`` IAM Policy must be attached

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_with_fargate_profile.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eks_create_fargate_profile]
    :end-before: [END howto_operator_eks_create_fargate_profile]

.. _howto/operator:EksDeleteFargateProfileOperator:

Delete an AWS Fargate Profile
=============================

To delete an existing AWS Fargate Profile you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EksDeleteFargateProfileOperator`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_with_fargate_profile.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eks_delete_fargate_profile]
    :end-before: [END howto_operator_eks_delete_fargate_profile]

.. _howto/operator:EksPodOperator:

Perform a Task on an Amazon EKS Cluster
=======================================

To run a pod on an existing Amazon EKS Cluster, you can use
:class:`~airflow.providers.amazon.aws.operators.eks.EksPodOperator`.

Note: An Amazon EKS Cluster with underlying compute infrastructure is required.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_eks_with_nodegroups.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eks_pod_operator]
    :end-before: [END howto_operator_eks_pod_operator]

Sensors
-------

.. _howto/sensor:EksClusterStateSensor:

Wait on an Amazon EKS cluster state
===================================

To check the state of an Amazon EKS Cluster until it reaches the target state or another terminal
state you can use :class:`~airflow.providers.amazon.aws.sensors.eks.EksClusterStateSensor`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_eks_with_nodegroups.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_eks_cluster]
    :end-before: [END howto_sensor_eks_cluster]

.. _howto/sensor:EksNodegroupStateSensor:

Wait on an Amazon EKS managed node group state
==============================================

To check the state of an Amazon EKS managed node group until it reaches the target state or another terminal
state you can use :class:`~airflow.providers.amazon.aws.sensors.eks.EksNodegroupStateSensor`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_eks_with_nodegroups.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_eks_nodegroup]
    :end-before: [END howto_sensor_eks_nodegroup]

.. _howto/sensor:EksFargateProfileStateSensor:

Wait on an AWS Fargate profile state
====================================

To check the state of an AWS Fargate profile until it reaches the target state or another terminal
state you can use :class:`~airflow.providers.amazon.aws.sensors.eks.EksFargateProfileSensor`.

.. exampleinclude:: /../../airflow/providers/amazon/aws/example_dags/example_eks_with_fargate_profile.py
    :language: python
    :dedent: 4
    :start-after: [START howto_sensor_eks_fargate]
    :end-before: [END howto_sensor_eks_fargate]

Reference
---------

* `AWS boto3 library documentation for EKS <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/eks.html>`__
