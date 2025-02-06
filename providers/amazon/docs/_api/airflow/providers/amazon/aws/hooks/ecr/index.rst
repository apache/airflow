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

:py:mod:`airflow.providers.amazon.aws.hooks.ecr`
================================================

.. py:module:: airflow.providers.amazon.aws.hooks.ecr


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.ecr.EcrCredentials
   airflow.providers.amazon.aws.hooks.ecr.EcrHook




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.ecr.logger


.. py:data:: logger



.. py:class:: EcrCredentials


   Helper (frozen dataclass) for storing temporary ECR credentials.

   .. py:property:: registry
      :type: str

      Return registry in appropriate `docker login` format.


   .. py:attribute:: username
      :type: str



   .. py:attribute:: password
      :type: str



   .. py:attribute:: proxy_endpoint
      :type: str



   .. py:attribute:: expires_at
      :type: datetime.datetime



   .. py:method:: __post_init__()



.. py:class:: EcrHook(**kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Elastic Container Registry (ECR).

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("ecr") <ECR.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:method:: get_temporary_credentials(registry_ids = None)

      Get temporary credentials for Amazon ECR.

      .. seealso::
          - :external+boto3:py:meth:`ECR.Client.get_authorization_token`

      :param registry_ids: Either AWS Account ID or list of AWS Account IDs that are associated
          with the registries from which credentials are obtained. If you do not specify a registry,
          the default registry is assumed.
      :return: list of :class:`airflow.providers.amazon.aws.hooks.ecr.EcrCredentials`,
          obtained credentials valid for 12 hours.
