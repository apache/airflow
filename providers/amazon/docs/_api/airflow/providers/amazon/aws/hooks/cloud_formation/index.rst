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

:py:mod:`airflow.providers.amazon.aws.hooks.cloud_formation`
============================================================

.. py:module:: airflow.providers.amazon.aws.hooks.cloud_formation

.. autoapi-nested-parse::

   This module contains AWS CloudFormation Hook.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.cloud_formation.CloudFormationHook




.. py:class:: CloudFormationHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS CloudFormation.

   Provide thin wrapper around
   :external+boto3:py:class:`boto3.client("cloudformation") <CloudFormation.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:method:: get_stack_status(stack_name)

      Get stack status from CloudFormation.

      .. seealso::
          - :external+boto3:py:meth:`CloudFormation.Client.describe_stacks`



   .. py:method:: create_stack(stack_name, cloudformation_parameters)

      Create stack in CloudFormation.

      .. seealso::
          - :external+boto3:py:meth:`CloudFormation.Client.create_stack`

      :param stack_name: stack_name.
      :param cloudformation_parameters: parameters to be passed to CloudFormation.


   .. py:method:: delete_stack(stack_name, cloudformation_parameters = None)

      Delete stack in CloudFormation.

      .. seealso::
          - :external+boto3:py:meth:`CloudFormation.Client.delete_stack`

      :param stack_name: stack_name.
      :param cloudformation_parameters: parameters to be passed to CloudFormation (optional).
