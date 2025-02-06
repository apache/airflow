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

:py:mod:`airflow.providers.amazon.aws.hooks.ec2`
================================================

.. py:module:: airflow.providers.amazon.aws.hooks.ec2


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.ec2.EC2Hook



Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.ec2.only_client_type



.. py:function:: only_client_type(func)


.. py:class:: EC2Hook(api_type='resource_type', *args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Elastic Compute Cloud (EC2).

   Provide thick wrapper around :external+boto3:py:class:`boto3.client("ec2") <EC2.Client>`
   or :external+boto3:py:class:`boto3.resource("ec2") <EC2.ServiceResource>`.

   :param api_type: If set to ``client_type`` then hook use ``boto3.client("ec2")`` capabilities,
       If set to ``resource_type`` then hook use ``boto3.resource("ec2")`` capabilities.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:attribute:: API_TYPES



   .. py:method:: get_instance(instance_id, filters = None)

      Get EC2 instance by id and return it.

      :param instance_id: id of the AWS EC2 instance
      :param filters: List of filters to specify instances to get
      :return: Instance object


   .. py:method:: stop_instances(instance_ids)

      Stop instances with given ids.

      :param instance_ids: List of instance ids to stop
      :return: Dict with key `StoppingInstances` and value as list of instances being stopped


   .. py:method:: start_instances(instance_ids)

      Start instances with given ids.

      :param instance_ids: List of instance ids to start
      :return: Dict with key `StartingInstances` and value as list of instances being started


   .. py:method:: terminate_instances(instance_ids)

      Terminate instances with given ids.

      :param instance_ids: List of instance ids to terminate
      :return: Dict with key `TerminatingInstances` and value as list of instances being terminated


   .. py:method:: describe_instances(filters = None, instance_ids = None)

      Describe EC2 instances, optionally applying filters and selective instance ids.

      :param filters: List of filters to specify instances to describe
      :param instance_ids: List of instance IDs to describe
      :return: Response from EC2 describe_instances API


   .. py:method:: get_instances(filters = None, instance_ids = None)

      Get list of instance details, optionally applying filters and selective instance ids.

      :param instance_ids: List of ids to get instances for
      :param filters: List of filters to specify instances to get
      :return: List of instances


   .. py:method:: get_instance_ids(filters = None)

      Get list of instance ids, optionally applying filters to fetch selective instances.

      :param filters: List of filters to specify instances to get
      :return: List of instance ids


   .. py:method:: get_instance_state_async(instance_id)
      :async:


   .. py:method:: get_instance_state(instance_id)

      Get EC2 instance state by id and return it.

      :param instance_id: id of the AWS EC2 instance
      :return: current state of the instance


   .. py:method:: wait_for_state(instance_id, target_state, check_interval)

      Wait EC2 instance until its state is equal to the target_state.

      :param instance_id: id of the AWS EC2 instance
      :param target_state: target state of instance
      :param check_interval: time in seconds that the job should wait in
          between each instance state checks until operation is completed
      :return: None
