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

:py:mod:`airflow.providers.amazon.aws.hooks.quicksight`
=======================================================

.. py:module:: airflow.providers.amazon.aws.hooks.quicksight


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.quicksight.QuickSightHook




.. py:class:: QuickSightHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon QuickSight.

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("quicksight") <QuickSight.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:attribute:: NON_TERMINAL_STATES



   .. py:attribute:: FAILED_STATES



   .. py:method:: sts_hook()


   .. py:method:: create_ingestion(data_set_id, ingestion_id, ingestion_type, wait_for_completion = True, check_interval = 30)

      Create and start a new SPICE ingestion for a dataset; refresh the SPICE datasets.

      .. seealso::
          - :external+boto3:py:meth:`QuickSight.Client.create_ingestion`

      :param data_set_id:  ID of the dataset used in the ingestion.
      :param ingestion_id: ID for the ingestion.
      :param ingestion_type: Type of ingestion . "INCREMENTAL_REFRESH"|"FULL_REFRESH"
      :param wait_for_completion: if the program should keep running until job finishes
      :param check_interval: the time interval in seconds which the operator
          will check the status of QuickSight Ingestion
      :return: Returns descriptive information about the created data ingestion
          having Ingestion ARN, HTTP status, ingestion ID and ingestion status.


   .. py:method:: get_status(aws_account_id, data_set_id, ingestion_id)

      Get the current status of QuickSight Create Ingestion API.

      .. seealso::
          - :external+boto3:py:meth:`QuickSight.Client.describe_ingestion`

      :param aws_account_id: An AWS Account ID
      :param data_set_id: QuickSight Data Set ID
      :param ingestion_id: QuickSight Ingestion ID
      :return: An QuickSight Ingestion Status


   .. py:method:: get_error_info(aws_account_id, data_set_id, ingestion_id)

      Get info about the error if any.

      :param aws_account_id: An AWS Account ID
      :param data_set_id: QuickSight Data Set ID
      :param ingestion_id: QuickSight Ingestion ID
      :return: Error info dict containing the error type (key 'Type') and message (key 'Message')
          if available. Else, returns None.


   .. py:method:: wait_for_state(aws_account_id, data_set_id, ingestion_id, target_state, check_interval)

      Check status of a QuickSight Create Ingestion API.

      :param aws_account_id: An AWS Account ID
      :param data_set_id: QuickSight Data Set ID
      :param ingestion_id: QuickSight Ingestion ID
      :param target_state: Describes the QuickSight Job's Target State
      :param check_interval: the time interval in seconds which the operator
          will check the status of QuickSight Ingestion
      :return: response of describe_ingestion call after Ingestion is done
