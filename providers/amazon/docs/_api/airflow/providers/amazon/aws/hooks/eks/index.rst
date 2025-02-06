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

:py:mod:`airflow.providers.amazon.aws.hooks.eks`
================================================

.. py:module:: airflow.providers.amazon.aws.hooks.eks

.. autoapi-nested-parse::

   Interact with Amazon EKS, using the boto3 library.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.eks.ClusterStates
   airflow.providers.amazon.aws.hooks.eks.FargateProfileStates
   airflow.providers.amazon.aws.hooks.eks.NodegroupStates
   airflow.providers.amazon.aws.hooks.eks.EksHook




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.eks.DEFAULT_PAGINATION_TOKEN
   airflow.providers.amazon.aws.hooks.eks.STS_TOKEN_EXPIRES_IN
   airflow.providers.amazon.aws.hooks.eks.AUTHENTICATION_API_VERSION


.. py:data:: DEFAULT_PAGINATION_TOKEN
   :value: ''



.. py:data:: STS_TOKEN_EXPIRES_IN
   :value: 60



.. py:data:: AUTHENTICATION_API_VERSION
   :value: 'client.authentication.k8s.io/v1alpha1'



.. py:class:: ClusterStates


   Bases: :py:obj:`enum.Enum`

   Contains the possible State values of an EKS Cluster.

   .. py:attribute:: CREATING
      :value: 'CREATING'



   .. py:attribute:: ACTIVE
      :value: 'ACTIVE'



   .. py:attribute:: DELETING
      :value: 'DELETING'



   .. py:attribute:: FAILED
      :value: 'FAILED'



   .. py:attribute:: UPDATING
      :value: 'UPDATING'



   .. py:attribute:: NONEXISTENT
      :value: 'NONEXISTENT'




.. py:class:: FargateProfileStates


   Bases: :py:obj:`enum.Enum`

   Contains the possible State values of an AWS Fargate profile.

   .. py:attribute:: CREATING
      :value: 'CREATING'



   .. py:attribute:: ACTIVE
      :value: 'ACTIVE'



   .. py:attribute:: DELETING
      :value: 'DELETING'



   .. py:attribute:: CREATE_FAILED
      :value: 'CREATE_FAILED'



   .. py:attribute:: DELETE_FAILED
      :value: 'DELETE_FAILED'



   .. py:attribute:: NONEXISTENT
      :value: 'NONEXISTENT'




.. py:class:: NodegroupStates


   Bases: :py:obj:`enum.Enum`

   Contains the possible State values of an EKS Managed Nodegroup.

   .. py:attribute:: CREATING
      :value: 'CREATING'



   .. py:attribute:: ACTIVE
      :value: 'ACTIVE'



   .. py:attribute:: UPDATING
      :value: 'UPDATING'



   .. py:attribute:: DELETING
      :value: 'DELETING'



   .. py:attribute:: CREATE_FAILED
      :value: 'CREATE_FAILED'



   .. py:attribute:: DELETE_FAILED
      :value: 'DELETE_FAILED'



   .. py:attribute:: DEGRADED
      :value: 'DEGRADED'



   .. py:attribute:: NONEXISTENT
      :value: 'NONEXISTENT'




.. py:class:: EksHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Elastic Kubernetes Service (EKS).

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("eks") <EKS.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:attribute:: client_type
      :value: 'eks'



   .. py:method:: create_cluster(name, roleArn, resourcesVpcConfig, **kwargs)

      Create an Amazon EKS control plane.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.create_cluster`

      :param name: The unique name to give to your Amazon EKS Cluster.
      :param roleArn: The Amazon Resource Name (ARN) of the IAM role that provides permissions
        for the Kubernetes control plane to make calls to AWS API operations on your behalf.
      :param resourcesVpcConfig: The VPC configuration used by the cluster control plane.
      :return: Returns descriptive information about the created EKS Cluster.


   .. py:method:: create_nodegroup(clusterName, nodegroupName, subnets, nodeRole, *, tags = None, **kwargs)

      Create an Amazon EKS managed node group for an Amazon EKS Cluster.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.create_nodegroup`

      :param clusterName: The name of the Amazon EKS cluster to create the EKS Managed Nodegroup in.
      :param nodegroupName: The unique name to give your managed nodegroup.
      :param subnets: The subnets to use for the Auto Scaling group that is created for your nodegroup.
      :param nodeRole: The Amazon Resource Name (ARN) of the IAM role to associate with your nodegroup.
      :param tags: Optional tags to apply to your nodegroup.
      :return: Returns descriptive information about the created EKS Managed Nodegroup.


   .. py:method:: create_fargate_profile(clusterName, fargateProfileName, podExecutionRoleArn, selectors, **kwargs)

      Create an AWS Fargate profile for an Amazon EKS cluster.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.create_fargate_profile`

      :param clusterName: The name of the Amazon EKS cluster to apply the Fargate profile to.
      :param fargateProfileName: The name of the Fargate profile.
      :param podExecutionRoleArn: The Amazon Resource Name (ARN) of the pod execution role to
          use for pods that match the selectors in the Fargate profile.
      :param selectors: The selectors to match for pods to use this Fargate profile.
      :return: Returns descriptive information about the created Fargate profile.


   .. py:method:: delete_cluster(name)

      Delete the Amazon EKS Cluster control plane.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.delete_cluster`

      :param name: The name of the cluster to delete.
      :return: Returns descriptive information about the deleted EKS Cluster.


   .. py:method:: delete_nodegroup(clusterName, nodegroupName)

      Delete an Amazon EKS managed node group from a specified cluster.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.delete_nodegroup`

      :param clusterName: The name of the Amazon EKS Cluster that is associated with your nodegroup.
      :param nodegroupName: The name of the nodegroup to delete.
      :return: Returns descriptive information about the deleted EKS Managed Nodegroup.


   .. py:method:: delete_fargate_profile(clusterName, fargateProfileName)

      Delete an AWS Fargate profile from a specified Amazon EKS cluster.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.delete_fargate_profile`

      :param clusterName: The name of the Amazon EKS cluster associated with the Fargate profile to delete.
      :param fargateProfileName: The name of the Fargate profile to delete.
      :return: Returns descriptive information about the deleted Fargate profile.


   .. py:method:: describe_cluster(name, verbose = False)

      Return descriptive information about an Amazon EKS Cluster.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.describe_cluster`

      :param name: The name of the cluster to describe.
      :param verbose: Provides additional logging if set to True.  Defaults to False.
      :return: Returns descriptive information about a specific EKS Cluster.


   .. py:method:: describe_nodegroup(clusterName, nodegroupName, verbose = False)

      Return descriptive information about an Amazon EKS managed node group.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.describe_nodegroup`

      :param clusterName: The name of the Amazon EKS Cluster associated with the nodegroup.
      :param nodegroupName: The name of the nodegroup to describe.
      :param verbose: Provides additional logging if set to True.  Defaults to False.
      :return: Returns descriptive information about a specific EKS Nodegroup.


   .. py:method:: describe_fargate_profile(clusterName, fargateProfileName, verbose = False)

      Return descriptive information about an AWS Fargate profile.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.describe_fargate_profile`

      :param clusterName: The name of the Amazon EKS Cluster associated with the Fargate profile.
      :param fargateProfileName: The name of the Fargate profile to describe.
      :param verbose: Provides additional logging if set to True.  Defaults to False.
      :return: Returns descriptive information about an AWS Fargate profile.


   .. py:method:: get_cluster_state(clusterName)

      Return the current status of a given Amazon EKS Cluster.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.describe_cluster`

      :param clusterName: The name of the cluster to check.
      :return: Returns the current status of a given Amazon EKS Cluster.


   .. py:method:: get_fargate_profile_state(clusterName, fargateProfileName)

      Return the current status of a given AWS Fargate profile.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.describe_fargate_profile`

      :param clusterName: The name of the Amazon EKS Cluster associated with the Fargate profile.
      :param fargateProfileName: The name of the Fargate profile to check.
      :return: Returns the current status of a given AWS Fargate profile.


   .. py:method:: get_nodegroup_state(clusterName, nodegroupName)

      Return the current status of a given Amazon EKS managed node group.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.describe_nodegroup`

      :param clusterName: The name of the Amazon EKS Cluster associated with the nodegroup.
      :param nodegroupName: The name of the nodegroup to check.
      :return: Returns the current status of a given Amazon EKS Nodegroup.


   .. py:method:: list_clusters(verbose = False)

      List all Amazon EKS Clusters in your AWS account.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.list_clusters`

      :param verbose: Provides additional logging if set to True.  Defaults to False.
      :return: A List containing the cluster names.


   .. py:method:: list_nodegroups(clusterName, verbose = False)

      List all Amazon EKS managed node groups associated with the specified cluster.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.list_nodegroups`

      :param clusterName: The name of the Amazon EKS Cluster containing nodegroups to list.
      :param verbose: Provides additional logging if set to True.  Defaults to False.
      :return: A List of nodegroup names within the given cluster.


   .. py:method:: list_fargate_profiles(clusterName, verbose = False)

      List all AWS Fargate profiles associated with the specified cluster.

      .. seealso::
          - :external+boto3:py:meth:`EKS.Client.list_fargate_profiles`

      :param clusterName: The name of the Amazon EKS Cluster containing Fargate profiles to list.
      :param verbose: Provides additional logging if set to True.  Defaults to False.
      :return: A list of Fargate profile names within a given cluster.


   .. py:method:: generate_config_file(eks_cluster_name, pod_namespace)

      Write the kubeconfig file given an EKS Cluster.

      :param eks_cluster_name: The name of the cluster to generate kubeconfig file for.
      :param pod_namespace: The namespace to run within kubernetes.


   .. py:method:: fetch_access_token_for_cluster(eks_cluster_name)
