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

:py:mod:`airflow.providers.amazon.aws.operators.eks`
====================================================

.. py:module:: airflow.providers.amazon.aws.operators.eks

.. autoapi-nested-parse::

   This module contains Amazon EKS operators.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.eks.EksCreateClusterOperator
   airflow.providers.amazon.aws.operators.eks.EksCreateNodegroupOperator
   airflow.providers.amazon.aws.operators.eks.EksCreateFargateProfileOperator
   airflow.providers.amazon.aws.operators.eks.EksDeleteClusterOperator
   airflow.providers.amazon.aws.operators.eks.EksDeleteNodegroupOperator
   airflow.providers.amazon.aws.operators.eks.EksDeleteFargateProfileOperator
   airflow.providers.amazon.aws.operators.eks.EksPodOperator




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.eks.CHECK_INTERVAL_SECONDS
   airflow.providers.amazon.aws.operators.eks.TIMEOUT_SECONDS
   airflow.providers.amazon.aws.operators.eks.DEFAULT_COMPUTE_TYPE
   airflow.providers.amazon.aws.operators.eks.DEFAULT_CONN_ID
   airflow.providers.amazon.aws.operators.eks.DEFAULT_FARGATE_PROFILE_NAME
   airflow.providers.amazon.aws.operators.eks.DEFAULT_NAMESPACE_NAME
   airflow.providers.amazon.aws.operators.eks.DEFAULT_NODEGROUP_NAME
   airflow.providers.amazon.aws.operators.eks.CAN_NOT_DELETE_MSG
   airflow.providers.amazon.aws.operators.eks.MISSING_ARN_MSG
   airflow.providers.amazon.aws.operators.eks.SUCCESS_MSG
   airflow.providers.amazon.aws.operators.eks.SUPPORTED_COMPUTE_VALUES
   airflow.providers.amazon.aws.operators.eks.NODEGROUP_FULL_NAME
   airflow.providers.amazon.aws.operators.eks.FARGATE_FULL_NAME


.. py:data:: CHECK_INTERVAL_SECONDS
   :value: 15



.. py:data:: TIMEOUT_SECONDS



.. py:data:: DEFAULT_COMPUTE_TYPE
   :value: 'nodegroup'



.. py:data:: DEFAULT_CONN_ID
   :value: 'aws_default'



.. py:data:: DEFAULT_FARGATE_PROFILE_NAME
   :value: 'profile'



.. py:data:: DEFAULT_NAMESPACE_NAME
   :value: 'default'



.. py:data:: DEFAULT_NODEGROUP_NAME
   :value: 'nodegroup'



.. py:data:: CAN_NOT_DELETE_MSG
   :value: 'A cluster can not be deleted with attached {compute}.  Deleting {count} {compute}.'



.. py:data:: MISSING_ARN_MSG
   :value: 'Creating an {compute} requires {requirement} to be passed in.'



.. py:data:: SUCCESS_MSG
   :value: 'No {compute} remain, deleting cluster.'



.. py:data:: SUPPORTED_COMPUTE_VALUES



.. py:data:: NODEGROUP_FULL_NAME
   :value: 'Amazon EKS managed node groups'



.. py:data:: FARGATE_FULL_NAME
   :value: 'AWS Fargate profiles'



.. py:class:: EksCreateClusterOperator(cluster_name, cluster_role_arn, resources_vpc_config, compute = DEFAULT_COMPUTE_TYPE, create_cluster_kwargs = None, nodegroup_name = DEFAULT_NODEGROUP_NAME, nodegroup_role_arn = None, create_nodegroup_kwargs = None, fargate_profile_name = DEFAULT_FARGATE_PROFILE_NAME, fargate_pod_execution_role_arn = None, fargate_selectors = None, create_fargate_profile_kwargs = None, wait_for_completion = False, aws_conn_id = DEFAULT_CONN_ID, region = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), waiter_delay = 30, waiter_max_attempts = 40, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Creates an Amazon EKS Cluster control plane.

   Optionally, can also create the supporting compute architecture:

    - If argument 'compute' is provided with a value of 'nodegroup', will also
        attempt to create an Amazon EKS Managed Nodegroup for the cluster.
        See :class:`~airflow.providers.amazon.aws.operators.EksCreateNodegroupOperator`
        documentation for requirements.

   -  If argument 'compute' is provided with a value of 'fargate', will also attempt to create an AWS
        Fargate profile for the cluster.
        See :class:`~airflow.providers.amazon.aws.operators.EksCreateFargateProfileOperator`
        documentation for requirements.


   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EksCreateClusterOperator`

   :param cluster_name: The unique name to give to your Amazon EKS Cluster. (templated)
   :param cluster_role_arn: The Amazon Resource Name (ARN) of the IAM role that provides permissions for the
        Kubernetes control plane to make calls to AWS API operations on your behalf. (templated)
   :param resources_vpc_config: The VPC configuration used by the cluster control plane. (templated)
   :param compute: The type of compute architecture to generate along with the cluster. (templated)
        Defaults to 'nodegroup' to generate an EKS Managed Nodegroup.
   :param create_cluster_kwargs: Optional parameters to pass to the CreateCluster API (templated)
   :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)
   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).
   :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.

   If compute is assigned the value of 'nodegroup':

   :param nodegroup_name: *REQUIRED* The unique name to give your Amazon EKS managed node group. (templated)
   :param nodegroup_role_arn: *REQUIRED* The Amazon Resource Name (ARN) of the IAM role to associate with
        the Amazon EKS managed node group. (templated)
   :param create_nodegroup_kwargs: Optional parameters to pass to the CreateNodegroup API (templated)


   If compute is assigned the value of 'fargate':

   :param fargate_profile_name: *REQUIRED* The unique name to give your AWS Fargate profile. (templated)
   :param fargate_pod_execution_role_arn: *REQUIRED* The Amazon Resource Name (ARN) of the pod execution
        role to use for pods that match the selectors in the AWS Fargate profile. (templated)
   :param fargate_selectors: The selectors to match for pods to use this AWS Fargate profile. (templated)
   :param create_fargate_profile_kwargs: Optional parameters to pass to the CreateFargateProfile API
        (templated)
   :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check cluster state
   :param waiter_max_attempts: The maximum number of attempts to check cluster state
   :param deferrable: If True, the operator will wait asynchronously for the job to complete.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)


   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'cluster_role_arn', 'resources_vpc_config', 'create_cluster_kwargs', 'compute',...



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: deferrable_create_cluster_next(context, event = None)


   .. py:method:: execute_failed(context, event = None)


   .. py:method:: execute_complete(context, event = None)



.. py:class:: EksCreateNodegroupOperator(cluster_name, nodegroup_subnets, nodegroup_role_arn, nodegroup_name = DEFAULT_NODEGROUP_NAME, create_nodegroup_kwargs = None, wait_for_completion = False, aws_conn_id = DEFAULT_CONN_ID, region = None, waiter_delay = 30, waiter_max_attempts = 80, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Creates an Amazon EKS managed node group for an existing Amazon EKS Cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EksCreateNodegroupOperator`

   :param cluster_name: The name of the Amazon EKS Cluster to create the managed nodegroup in. (templated)
   :param nodegroup_name: The unique name to give your managed nodegroup. (templated)
   :param nodegroup_subnets:
        The subnets to use for the Auto Scaling group that is created for the managed nodegroup. (templated)
   :param nodegroup_role_arn:
        The Amazon Resource Name (ARN) of the IAM role to associate with the managed nodegroup. (templated)
   :param create_nodegroup_kwargs: Optional parameters to pass to the Create Nodegroup API (templated)
   :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)
   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).
   :param region: Which AWS region the connection should use. (templated)
       If this is None or empty then the default boto3 behaviour is used.
   :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check nodegroup state
   :param waiter_max_attempts: The maximum number of attempts to check nodegroup state
   :param deferrable: If True, the operator will wait asynchronously for the nodegroup to be created.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)


   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'nodegroup_subnets', 'nodegroup_role_arn', 'nodegroup_name',...



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: EksCreateFargateProfileOperator(cluster_name, pod_execution_role_arn, selectors, fargate_profile_name = DEFAULT_FARGATE_PROFILE_NAME, create_fargate_profile_kwargs = None, wait_for_completion = False, aws_conn_id = DEFAULT_CONN_ID, region = None, waiter_delay = 10, waiter_max_attempts = 60, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Creates an AWS Fargate profile for an Amazon EKS cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EksCreateFargateProfileOperator`

   :param cluster_name: The name of the Amazon EKS cluster to apply the AWS Fargate profile to. (templated)
   :param pod_execution_role_arn: The Amazon Resource Name (ARN) of the pod execution role to
        use for pods that match the selectors in the AWS Fargate profile. (templated)
   :param selectors: The selectors to match for pods to use this AWS Fargate profile. (templated)
   :param fargate_profile_name: The unique name to give your AWS Fargate profile. (templated)
   :param create_fargate_profile_kwargs: Optional parameters to pass to the CreateFargate Profile API
    (templated)
   :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)

   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).
   :param region: Which AWS region the connection should use. (templated)
       If this is None or empty then the default boto3 behaviour is used.
   :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check profile status
   :param waiter_max_attempts: The maximum number of attempts to check the status of the profile.
   :param deferrable: If True, the operator will wait asynchronously for the profile to be created.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'pod_execution_role_arn', 'selectors', 'fargate_profile_name',...



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: EksDeleteClusterOperator(cluster_name, force_delete_compute = False, wait_for_completion = False, aws_conn_id = DEFAULT_CONN_ID, region = None, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), waiter_delay = 30, waiter_max_attempts = 40, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Deletes the Amazon EKS Cluster control plane and all nodegroups attached to it.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EksDeleteClusterOperator`

   :param cluster_name: The name of the Amazon EKS Cluster to delete. (templated)
   :param force_delete_compute: If True, will delete any attached resources. (templated)
        Defaults to False.
   :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)
   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).
   :param region: Which AWS region the connection should use. (templated)
       If this is None or empty then the default boto3 behaviour is used.
   :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check cluster state
   :param waiter_max_attempts: The maximum number of attempts to check cluster state
   :param deferrable: If True, the operator will wait asynchronously for the cluster to be deleted.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)


   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'force_delete_compute', 'wait_for_completion', 'aws_conn_id', 'region')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: delete_any_nodegroups(eks_hook)

      Delete all Amazon EKS managed node groups for a provided Amazon EKS Cluster.

      Amazon EKS managed node groups can be deleted in parallel, so we can send all
      delete commands in bulk and move on once the count of nodegroups is zero.


   .. py:method:: delete_any_fargate_profiles(eks_hook)

      Delete all EKS Fargate profiles for a provided Amazon EKS Cluster.

      EKS Fargate profiles must be deleted one at a time, so we must wait
      for one to be deleted before sending the next delete command.


   .. py:method:: execute_complete(context, event = None)



.. py:class:: EksDeleteNodegroupOperator(cluster_name, nodegroup_name, wait_for_completion = False, aws_conn_id = DEFAULT_CONN_ID, region = None, waiter_delay = 30, waiter_max_attempts = 40, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Deletes an Amazon EKS managed node group from an Amazon EKS Cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EksDeleteNodegroupOperator`

   :param cluster_name: The name of the Amazon EKS Cluster associated with your nodegroup. (templated)
   :param nodegroup_name: The name of the nodegroup to delete. (templated)
   :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)
   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used.  If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).
   :param region: Which AWS region the connection should use. (templated)
       If this is None or empty then the default boto3 behaviour is used.
   :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check nodegroup state
   :param waiter_max_attempts: The maximum number of attempts to check nodegroup state
   :param deferrable: If True, the operator will wait asynchronously for the nodegroup to be deleted.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)


   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'nodegroup_name', 'wait_for_completion', 'aws_conn_id', 'region')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: EksDeleteFargateProfileOperator(cluster_name, fargate_profile_name, wait_for_completion = False, aws_conn_id = DEFAULT_CONN_ID, region = None, waiter_delay = 30, waiter_max_attempts = 60, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Deletes an AWS Fargate profile from an Amazon EKS Cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EksDeleteFargateProfileOperator`

   :param cluster_name: The name of the Amazon EKS cluster associated with your Fargate profile. (templated)
   :param fargate_profile_name: The name of the AWS Fargate profile to delete. (templated)
   :param wait_for_completion: If True, waits for operator to complete. (default: False) (templated)
   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used.  If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).
   :param region: Which AWS region the connection should use. (templated)
       If this is None or empty then the default boto3 behaviour is used.
   :param waiter_delay: Time (in seconds) to wait between two consecutive calls to check profile status
   :param waiter_max_attempts: The maximum number of attempts to check the status of the profile.
   :param deferrable: If True, the operator will wait asynchronously for the profile to be deleted.
       This implies waiting for completion. This mode requires aiobotocore module to be installed.
       (default: False)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('cluster_name', 'fargate_profile_name', 'wait_for_completion', 'aws_conn_id', 'region')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event=None)



.. py:class:: EksPodOperator(cluster_name, in_cluster = False, namespace = DEFAULT_NAMESPACE_NAME, pod_context = None, pod_name = None, pod_username = None, aws_conn_id = DEFAULT_CONN_ID, region = None, on_finish_action = None, is_delete_operator_pod = None, **kwargs)


   Bases: :py:obj:`airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`

   Executes a task in a Kubernetes pod on the specified Amazon EKS Cluster.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EksPodOperator`

   :param cluster_name: The name of the Amazon EKS Cluster to execute the task on. (templated)
   :param in_cluster: If True, look for config inside the cluster; if False look for a local file path.
   :param namespace: The namespace in which to execute the pod. (templated)
   :param pod_name: The unique name to give the pod. (templated)
   :param aws_profile: The named profile containing the credentials for the AWS CLI tool to use.
   :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
   :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then the default boto3 configuration would be used (and must be
        maintained on each worker node).
   :param on_finish_action: What to do when the pod reaches its final state, or the execution is interrupted.
       If "delete_pod", the pod will be deleted regardless its state; if "delete_succeeded_pod",
       only succeeded pod will be deleted. You can set to "keep_pod" to keep the pod.
       Current default is `keep_pod`, but this will be changed in the next major release of this provider.
   :param is_delete_operator_pod: What to do when the pod reaches its final
       state, or the execution is interrupted. If True, delete the
       pod; if False, leave the pod. Current default is False, but this will be
       changed in the next major release of this provider.
       Deprecated - use `on_finish_action` instead.


   .. py:attribute:: template_fields
      :type: Sequence[str]



   .. py:method:: execute(context)

      Based on the deferrable parameter runs the pod asynchronously or synchronously.
