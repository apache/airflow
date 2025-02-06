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

:py:mod:`airflow.providers.amazon.aws.hooks.ssm`
================================================

.. py:module:: airflow.providers.amazon.aws.hooks.ssm


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.ssm.SsmHook




.. py:class:: SsmHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Systems Manager (SSM).

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("ssm") <SSM.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:method:: get_parameter_value(parameter, default = NOTSET)

      Return the provided Parameter or an optional default; if it is encrypted, then decrypt and mask.

      .. seealso::
          - :external+boto3:py:meth:`SSM.Client.get_parameter`

      :param parameter: The SSM Parameter name to return the value for.
      :param default: Optional default value to return if none is found.
