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

:py:mod:`airflow.providers.amazon.aws.hooks.glacier`
====================================================

.. py:module:: airflow.providers.amazon.aws.hooks.glacier


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.glacier.GlacierHook




.. py:class:: GlacierHook(aws_conn_id = 'aws_default')


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Glacier.

   This is a thin wrapper around
   :external+boto3:py:class:`boto3.client("glacier") <Glacier.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:method:: retrieve_inventory(vault_name)

      Initiate an Amazon Glacier inventory-retrieval job.

      .. seealso::
          - :external+boto3:py:meth:`Glacier.Client.initiate_job`

      :param vault_name: the Glacier vault on which job is executed


   .. py:method:: retrieve_inventory_results(vault_name, job_id)

      Retrieve the results of an Amazon Glacier inventory-retrieval job.

      .. seealso::
          - :external+boto3:py:meth:`Glacier.Client.get_job_output`

      :param vault_name: the Glacier vault on which job is executed
      :param job_id: the job ID was returned by retrieve_inventory()


   .. py:method:: describe_job(vault_name, job_id)

      Retrieve the status of an Amazon S3 Glacier job.

      .. seealso::
          - :external+boto3:py:meth:`Glacier.Client.describe_job`

      :param vault_name: the Glacier vault on which job is executed
      :param job_id: the job ID was returned by retrieve_inventory()
