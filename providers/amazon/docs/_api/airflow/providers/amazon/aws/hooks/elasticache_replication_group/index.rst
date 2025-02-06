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

:py:mod:`airflow.providers.amazon.aws.hooks.elasticache_replication_group`
==========================================================================

.. py:module:: airflow.providers.amazon.aws.hooks.elasticache_replication_group


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.elasticache_replication_group.ElastiCacheReplicationGroupHook




.. py:class:: ElastiCacheReplicationGroupHook(max_retries = 10, exponential_back_off_factor = 1, initial_poke_interval = 60, *args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon ElastiCache.

   Provide thick wrapper around :external+boto3:py:class:`boto3.client("elasticache") <ElastiCache.Client>`.

   :param max_retries: Max retries for checking availability of and deleting replication group
           If this is not supplied then this is defaulted to 10
   :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
           If this is not supplied then this is defaulted to 1
   :param initial_poke_interval: Initial sleep time in seconds
           If this is not supplied then this is defaulted to 60 seconds

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:attribute:: TERMINAL_STATES



   .. py:method:: create_replication_group(config)

      Create a Redis (cluster mode disabled) or a Redis (cluster mode enabled) replication group.

      .. seealso::
          - :external+boto3:py:meth:`ElastiCache.Client.create_replication_group`

      :param config: Configuration for creating the replication group
      :return: Response from ElastiCache create replication group API


   .. py:method:: delete_replication_group(replication_group_id)

      Delete an existing replication group.

      .. seealso::
          - :external+boto3:py:meth:`ElastiCache.Client.delete_replication_group`

      :param replication_group_id: ID of replication group to delete
      :return: Response from ElastiCache delete replication group API


   .. py:method:: describe_replication_group(replication_group_id)

      Get information about a particular replication group.

      .. seealso::
          - :external+boto3:py:meth:`ElastiCache.Client.describe_replication_groups`

      :param replication_group_id: ID of replication group to describe
      :return: Response from ElastiCache describe replication group API


   .. py:method:: get_replication_group_status(replication_group_id)

      Get current status of replication group.

      .. seealso::
          - :external+boto3:py:meth:`ElastiCache.Client.describe_replication_groups`

      :param replication_group_id: ID of replication group to check for status
      :return: Current status of replication group


   .. py:method:: is_replication_group_available(replication_group_id)

      Check if replication group is available or not.

      :param replication_group_id: ID of replication group to check for availability
      :return: True if available else False


   .. py:method:: wait_for_availability(replication_group_id, initial_sleep_time = None, exponential_back_off_factor = None, max_retries = None)

      Check if replication group is available or not by performing a describe over it.

      :param replication_group_id: ID of replication group to check for availability
      :param initial_sleep_time: Initial sleep time in seconds
          If this is not supplied then this is defaulted to class level value
      :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
          If this is not supplied then this is defaulted to class level value
      :param max_retries: Max retries for checking availability of replication group
          If this is not supplied then this is defaulted to class level value
      :return: True if replication is available else False


   .. py:method:: wait_for_deletion(replication_group_id, initial_sleep_time = None, exponential_back_off_factor = None, max_retries = None)

      Delete a replication group ensuring it is either deleted or can't be deleted.

      :param replication_group_id: ID of replication to delete
      :param initial_sleep_time: Initial sleep time in second
          If this is not supplied then this is defaulted to class level value
      :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
          If this is not supplied then this is defaulted to class level value
      :param max_retries: Max retries for checking availability of replication group
          If this is not supplied then this is defaulted to class level value
      :return: Response from ElastiCache delete replication group API and flag to identify if deleted or not


   .. py:method:: ensure_delete_replication_group(replication_group_id, initial_sleep_time = None, exponential_back_off_factor = None, max_retries = None)

      Delete a replication group ensuring it is either deleted or can't be deleted.

      :param replication_group_id: ID of replication to delete
      :param initial_sleep_time: Initial sleep time in second
          If this is not supplied then this is defaulted to class level value
      :param exponential_back_off_factor: Multiplication factor for deciding next sleep time
          If this is not supplied then this is defaulted to class level value
      :param max_retries: Max retries for checking availability of replication group
          If this is not supplied then this is defaulted to class level value
      :return: Response from ElastiCache delete replication group API
      :raises AirflowException: If replication group is not deleted
